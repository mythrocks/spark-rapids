package com.nvidia.spark.rapids.window

import ai.rapids.cudf
import ai.rapids.cudf.GroupByOptions
import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuBindReferences, GpuBoundReference, GpuColumnVector, GpuExec, GpuExpression, GpuOverrides, GpuProjectExec, RapidsConf, RapidsMeta, SparkPlanMeta}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, DenseRank, Expression, Rank, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.window.{WindowGroupLimitExec, WindowGroupLimitMode}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

sealed trait RankFunctionType
case object RankFunction extends RankFunctionType
case object DenseRankFunction extends RankFunctionType

class GpuWindowGroupLimitExecMeta(limitExec: WindowGroupLimitExec,
                                  conf: RapidsConf,
                                  parent:Option[RapidsMeta[_, _, _]],
                                  rule: DataFromReplacementRule)
    extends SparkPlanMeta[WindowGroupLimitExec](limitExec, conf, parent, rule) {

  private val partitionSpec: Seq[BaseExprMeta[Expression]] =
    limitExec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  private val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    limitExec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    wrapped.rankLikeFunction match {
      case DenseRank(_) =>
      case Rank(_) =>
      // case RowNumber() => // TODO: Future.
      case _ => willNotWorkOnGpu("Only Rank() and DenseRank() are " +
                                 "currently supported for window group limits")
    }
  }


  override def convertToGpu(): GpuExec = {
    GpuWindowGroupLimitExec(partitionSpec.map(_.convertToGpu()),
                            orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
                            limitExec.rankLikeFunction,
                            limitExec.limit,
                            limitExec.mode,
                            childPlans.head.convertIfNeeded())
  }
}

class GpuWindowGroupLimitingIterator(input: Iterator[ColumnarBatch],
                                     boundPartitionSpec: Seq[GpuExpression],
                                     boundOrderSpec: Seq[SortOrder],
                                     rankFunction: RankFunctionType,
                                     limit: Int)
  extends Iterator[ColumnarBatch]
  with Logging {

  override def hasNext: Boolean = input.hasNext

  // Caches input column schema on first read.
  private var inputTypes: Option[Array[DataType]] = None

  private val partByPositions: Array[Int] =
    boundPartitionSpec.map {_.asInstanceOf[GpuBoundReference].ordinal}.toArray
  private val sortColumns: Seq[Expression] = boundOrderSpec.map { _.child }

  private def readInputBatch = {
    def optionallySetInputTypes(inputCB: ColumnarBatch): Unit = {
      if (inputTypes.isEmpty) {
        inputTypes = Some(GpuColumnVector.extractTypes(inputCB))
      }
    }
    val inputCB = input.next()
    optionallySetInputTypes(inputCB)
    inputCB
  }

  override def next(): ColumnarBatch = {

    // TODO: 1. Make input columns spillable.
    // TODO: 2. Use opTime with NvtxWithMetrics. Refer to GpuBatchedBoundedWindowExec.

    if (!hasNext) {
      throw new NoSuchElementException()
    }

    withResource(readInputBatch) { inputCB =>
      val filterMap = withResource(calculateRankWithGroupByScan(rankFunction,
                                                                inputCB)) { ranks =>
        withResource(cudf.Scalar.fromInt(limit)) { limitScalar =>
          // For a query with `WHERE rank < n`, the limit passed to the exec
          // is `n - 1`.  The comparison should be `LESS_EQUAL`.
          ranks.binaryOp(cudf.BinaryOp.LESS_EQUAL, limitScalar, cudf.DType.BOOL8)
        }
      }

      withResource(filterMap) { _ =>
        withResource(GpuColumnVector.from(inputCB)) { inputTable =>
          withResource(inputTable.filter(filterMap)) {
            GpuColumnVector.from(_, inputTypes.get)
          }
        }
      }
    }
  }

  private def calculateRankWithGroupByScan(rankFunctionType: RankFunctionType,
                                           inputCB: ColumnarBatch): cudf.ColumnVector = {
    // For multiple order-by columns order-by columns, the group-scan's input projection
    // is a single STRUCT column (containing the order-by columns as members).
    val rankFunction = rankFunctionType match {
      case RankFunction => GpuRank(sortColumns)
      case DenseRankFunction => GpuDenseRank(sortColumns)
      case _ => throw new IllegalArgumentException("Unexpected ranking function")
    }
    val sortColumnProjection = rankFunction.scanInputProjection(isRunningBatched = false)

    // Append the projected sort-column to the end of the input table.
    val gbyScanInputTable = withResource(GpuColumnVector.from(inputCB)) { inputTable =>
      val sortColumn = withResource(GpuProjectExec.project(inputCB, sortColumnProjection)) {
        sortColumnCB => withResource(GpuColumnVector.from(sortColumnCB)) { sortColumnTable =>
          require(sortColumnTable.getNumberOfColumns == 1,
                  "Expected single (consolidated) sort-by column.")
          sortColumnTable.getColumn(0).incRefCount()
        }
      }
      withResource(sortColumn) { _ =>
        val columnsWithSortByAdded = Range(0, inputTable.getNumberOfColumns).map {
                                        inputTable.getColumn
                                      }.toArray :+ sortColumn
        new cudf.Table(columnsWithSortByAdded: _*)
      }
    }

    // Now, perform groupBy-scan:
    val sortColumnIndex = inputCB.numCols()
    val gbyScanAggregation = rankFunction.groupByScanAggregation(isRunningBatched = false).head.agg
    val sortedGroupByOptions = GroupByOptions.builder().withKeysSorted(true).build
    withResource(gbyScanInputTable) { _ =>
      withResource(gbyScanInputTable.groupBy(sortedGroupByOptions, partByPositions: _*)
                   .scan(gbyScanAggregation.onColumn(sortColumnIndex))) { gbyScanOutput =>
        // The last column should be the grouped ranks.
        gbyScanOutput.getColumn(gbyScanOutput.getNumberOfColumns - 1).incRefCount()
      }
    }
  }
}

case class GpuWindowGroupLimitExec(
    gpuPartitionSpec: Seq[Expression],
    gpuOrderSpec: Seq[SortOrder],
    rankLikeFunction: Expression,
    limit: Int,
    mode: WindowGroupLimitMode,
    child: SparkPlan) extends ShimUnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = child.output

  private def getRankFunctionType(expr: Expression): RankFunctionType = expr match {
    case Rank(_) => RankFunction
    case DenseRank(_) => DenseRankFunction
    case _ =>
      throw new UnsupportedOperationException("Only Rank() is currently supported for group limits")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // TODO: Populate metrics.
    //    val numOutputBatches = gpuLongMetric(GpuMetric.NUM_OUTPUT_BATCHES)
    //    val numOutputRows = gpuLongMetric(GpuMetric.NUM_OUTPUT_ROWS)
    //    val opTime = gpuLongMetric(GpuMetric.OP_TIME)

    val boundPartitionSpec = GpuBindReferences.bindGpuReferences(gpuPartitionSpec, child.output)
    val boundOrderSpec = GpuBindReferences.bindReferences(gpuOrderSpec, child.output)

    child.executeColumnar().mapPartitions { input =>
      new GpuWindowGroupLimitingIterator(input,
                                         boundPartitionSpec,
                                         boundOrderSpec,
                                         getRankFunctionType(rankLikeFunction),
                                         limit)
    }
  }

  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("Row-wise execution unsupported!")
}
