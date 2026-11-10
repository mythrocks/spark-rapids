/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids

import ai.rapids.cudf.DType
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.Partial
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.{GpuInputFileName, GpuInsertIntoHadoopFsRelationCommand, GpuOrcFileFormat}
import org.apache.spark.sql.rapids.aggregate.{GpuAggregateFunction, GpuCount, GpuFirst, GpuLast, GpuMax, GpuMin}
import org.apache.spark.sql.rapids.execution.{GpuBroadcastHashJoinExecBase, GpuShuffleExchangeExecBase}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A unary GPU expression that, given a StringType child backed by a STRING-keyed DICTIONARY32
 * column, returns a valid StringType result without reading the strings: it passes the column
 * through, possibly replacing nulls with a scalar. InputFileNameDictionaryRule lets a
 * dictionary-backed input_file_name() column flow through such an expression.
 */
trait GpuStringDictionaryPassThrough

/**
 * A unary GPU exec whose columnar output is its child's batches, unchanged.
 * InputFileNameDictionaryRule passes dictionary-backed input_file_name() columns through it.
 */
trait GpuBatchPassThroughExec

/**
 * Decides, for each `input_file_name()` in the plan, whether GpuInputFileName returns a
 * DICTIONARY32 column instead of a STRING column that repeats the file path on every row
 * (https://github.com/NVIDIA/cudf-spark/issues/13918).
 *
 * A dictionary-backed StringType column is only safe where every consumer can handle it: most
 * string, cast and binary operations, the writers and the shuffle serializers cannot. This walks
 * the plan from the root down, carrying the output columns of each operator that every operator
 * above it accepts as dictionaries. Dictionary output is turned on only for a top-level
 * `GpuAlias(GpuInputFileName(), name)` in a GpuProjectExec whose output column is in that set;
 * every other use keeps the STRING column.
 *
 * Operators that accept a dictionary column:
 *  - GpuColumnarToRowExec, unless it exports the columnar RDD. Rows read strings through
 *    RapidsHostColumnVectorCore, which looks up the key per row, or through
 *    RowConversion.convertToRows, which decodes inside a split-and-retry block.
 *  - GpuDataWritingCommandExec for an unpartitioned, unbucketed Parquet or ORC insert (see
 *    writesDecodedDictionaries), through the GpuWriteFilesLike node of planned writes.
 *    ColumnarOutputWriter decodes dictionaries inside a split-and-retry block before encoding.
 *  - GpuShuffleExchangeExecBase, except for range-partitioning keys. GpuPartitioning decodes
 *    dictionaries inside the exchange's split-and-retry block before slicing, so no serializer,
 *    no reduce side and no other query stage sees one.
 *  - A partial GpuHashAggregateExec (see aggregateAllowed), for grouping keys and the inputs of
 *    grouped count/min/max/first/last.
 *  - GpuBroadcastHashJoinExecBase (see streamSideAllowed), for stream-side columns that are
 *    not join keys or in the condition.
 *  - GpuProjectExec, for columns it passes through unchanged (possibly aliased or wrapped in a
 *    GpuStringDictionaryPassThrough expression).
 *  - GpuFilterExec, for columns its condition does not reference.
 *  - GpuCoalesceBatches, GpuLocalLimitExec, GpuGlobalLimitExec and GpuBatchPassThroughExec.
 *
 * The rule decides both ways, so applying it again to a re-optimized plan is safe.
 */
object InputFileNameDictionaryRule {

  def apply(plan: SparkPlan): SparkPlan = rewrite(plan, AttributeSet.empty)

  /**
   * @param plan    the plan to rewrite
   * @param allowed the output columns of `plan` that every operator above it accepts as
   *                dictionaries
   */
  private def rewrite(plan: SparkPlan, allowed: AttributeSet): SparkPlan = plan match {
    case c2r: GpuColumnarToRowExec =>
      val childAllowed = if (c2r.exportColumnarRdd) AttributeSet.empty else c2r.child.outputSet
      c2r.withNewChildren(Seq(rewrite(c2r.child, childAllowed)))
    case write: GpuDataWritingCommandExec if writesDecodedDictionaries(write.cmd) =>
      val newChild = write.child match {
        case writeFiles if writeFiles.isInstanceOf[GpuWriteFilesLike] =>
          val query = writeFiles.children.head
          writeFiles.withNewChildren(Seq(rewrite(query, query.outputSet)))
        case query =>
          rewrite(query, query.outputSet)
      }
      write.withNewChildren(Seq(newChild))
    case exchange: GpuShuffleExchangeExecBase =>
      // GpuRangePartitioner compares the range keys with STRING bounds before the decode.
      val rangeKeys = exchange.outputPartitioning match {
        case range: RangePartitioning => range.references
        case _ => AttributeSet.empty
      }
      exchange.withNewChildren(Seq(rewrite(exchange.child, exchange.child.outputSet -- rangeKeys)))
    case agg: GpuHashAggregateExec =>
      agg.withNewChildren(Seq(rewrite(agg.child, aggregateAllowed(agg, allowed))))
    case join: GpuBroadcastHashJoinExecBase =>
      val streamAllowed = streamSideAllowed(join, allowed)
      val (newLeft, newRight) = join.buildSide match {
        case GpuBuildRight =>
          (rewrite(join.left, streamAllowed), rewrite(join.right, AttributeSet.empty))
        case GpuBuildLeft =>
          (rewrite(join.left, AttributeSet.empty), rewrite(join.right, streamAllowed))
      }
      join.withNewChildren(Seq(newLeft, newRight))
    case project: GpuProjectExec =>
      rewriteProject(project, allowed)
    case filter: GpuFilterExec =>
      filter.withNewChildren(Seq(rewrite(filter.child, allowed -- filter.condition.references)))
    case coalesce: GpuCoalesceBatches =>
      coalesce.withNewChildren(Seq(rewrite(coalesce.child, allowed)))
    case limit: GpuLocalLimitExec =>
      limit.withNewChildren(Seq(rewrite(limit.child, allowed)))
    case limit: GpuGlobalLimitExec =>
      limit.withNewChildren(Seq(rewrite(limit.child, allowed)))
    case passThrough if passThrough.isInstanceOf[GpuBatchPassThroughExec] =>
      passThrough.withNewChildren(passThrough.children.map(rewrite(_, allowed)))
    case other =>
      other.withNewChildren(other.children.map(rewrite(_, AttributeSet.empty)))
  }

  /**
   * Whether every column a write command receives reaches ColumnarOutputWriter, which decodes
   * dictionaries, without any device operation in between. Only unpartitioned, unbucketed
   * Parquet and ORC inserts qualify. Dynamic partitions and buckets make GpuFileFormatWriter
   * sort the batches, possibly with a GpuSortExec it adds at run time where this rule cannot see
   * it, and the partitioned writers split and group the batches on the device. Hive and Delta
   * writes use other commands.
   */
  private def writesDecodedDictionaries(cmd: GpuDataWritingCommand): Boolean = cmd match {
    case insert: GpuInsertIntoHadoopFsRelationCommand =>
      insert.partitionColumns.isEmpty && insert.bucketSpec.isEmpty &&
        (insert.fileFormat.isInstanceOf[GpuParquetFileFormat] ||
          insert.fileFormat.isInstanceOf[GpuOrcFileFormat])
    case _ => false
  }

  /**
   * The child columns of a hash aggregate that may be dictionaries, given the aggregate's output
   * columns its parent accepts as dictionaries.
   *
   * Only a partial aggregate whose whole output is accepted qualifies (in practice, one below a
   * shuffle exchange, which decodes). libcudf groupby compares and hashes dictionary keys by
   * value, and returns grouping keys and min/max/first/last values as dictionaries, which then
   * reach the exchange. A column qualifies when it is used only as a bare grouping key or as the
   * direct input of count, or of min/max/first/last with grouping keys: their reductions throw on
   * a dictionary.
   *
   * When the single-pass sort path may run, grouping keys are excluded: GpuOutOfCoreSortIterator
   * compares the sorted batch's keys with a STRING cutoff, which throws on a dictionary key.
   * Values are only sort payload there, and the cutoff comparison reads keys only.
   */
  private def aggregateAllowed(agg: GpuHashAggregateExec, allowed: AttributeSet): AttributeSet = {
    val partial = agg.requiredChildDistributionExpressions.isEmpty &&
      agg.aggregateExpressions.forall(_.mode == Partial)
    if (!partial || !agg.output.forall(allowed.contains)) {
      AttributeSet.empty
    } else {
      val grouped = agg.groupingExpressions.nonEmpty
      val mayRunSorted = agg.allowSinglePassAgg || agg.forceSinglePassAgg
      val (keys, otherGrouping) = agg.groupingExpressions.partition { key =>
        !mayRunSorted && key.isInstanceOf[AttributeReference]
      }
      val inputs = agg.aggregateExpressions.map { e =>
        if (e.isDistinct || e.filter.isDefined) {
          None
        } else {
          dictionaryInput(e.aggregateFunction, grouped)
        }
      }
      val otherAggregates = agg.aggregateExpressions.zip(inputs).collect {
        case (e, None) => e
      }
      val otherReferences = AttributeSet(
        otherGrouping.flatMap(_.references) ++ otherAggregates.flatMap(_.references))
      AttributeSet(keys.map(_.toAttribute) ++ inputs.flatten) -- otherReferences
    }
  }

  /** The column an aggregate function reads directly, if it returns a dictionary unchanged. */
  private def dictionaryInput(
      function: GpuAggregateFunction,
      grouped: Boolean): Option[Attribute] = {
    val accepts = function match {
      case _: GpuCount => true
      case _: GpuMin | _: GpuMax | _: GpuFirst | _: GpuLast => grouped
      case _ => false
    }
    function.children match {
      case Seq(attr: AttributeReference) if accepts => Some(attr)
      case _ => None
    }
  }

  /**
   * The stream-side columns of a broadcast hash join that may be dictionaries. A stream-side
   * column is gathered unchanged into the output, and libcudf gathers dictionaries.
   * JoinGatherer sizes the output with DictionaryDecode.rowBitCount, which handles dictionaries.
   * Excluded:
   *  - join keys: libcudf throws on a dictionary key against a STRING key;
   *  - condition columns: AST conditions compare dictionary indices, and other conditions run
   *    string expressions;
   *  - join types that null-fill or empty the stream side (FullOuter, LeftOuter building left,
   *    RightOuter building right): the join builds those columns as STRING from the Spark type,
   *    and a later concatenate rejects a mix of STRING and dictionary batches.
   */
  private def streamSideAllowed(
      join: GpuBroadcastHashJoinExecBase,
      allowed: AttributeSet): AttributeSet = {
    val stream = join.buildSide match {
      case GpuBuildRight => join.left
      case GpuBuildLeft => join.right
    }
    if (!keepsStreamColumns(join.joinType, join.buildSide)) {
      AttributeSet.empty
    } else {
      val usedByJoin = AttributeSet((join.leftKeys ++ join.rightKeys ++ join.condition.toSeq)
        .flatMap(_.references))
      AttributeSet(stream.output.filter(allowed.contains)) -- usedByJoin
    }
  }

  private def keepsStreamColumns(joinType: JoinType, buildSide: GpuBuildSide): Boolean =
    joinType match {
      case Inner | LeftSemi | LeftAnti | ExistenceJoin(_) => true
      case LeftOuter => buildSide == GpuBuildRight
      case RightOuter => buildSide == GpuBuildLeft
      case _ => false
    }

  private def rewriteProject(project: GpuProjectExec, allowed: AttributeSet): SparkPlan = {
    val newProjectList = project.projectList.map {
      case alias @ GpuAlias(fileName: GpuInputFileName, _) =>
        val outputDictionary = allowed.contains(alias.toAttribute)
        if (fileName.outputDictionary == outputDictionary) {
          alias
        } else {
          alias.withNewChildren(Seq(fileName.copy(outputDictionary = outputDictionary)))
            .asInstanceOf[NamedExpression]
        }
      case other => other
    }

    // A child column stays allowed only if every use of it in this project is a pass-through to
    // an allowed output column.
    val passThroughs = project.projectList.flatMap { expr =>
      passThroughSource(expr).map(source => (source, expr.toAttribute))
    }
    val otherReferences = AttributeSet(
      project.projectList.filter(passThroughSource(_).isEmpty).flatMap(_.references))
    val childAllowed = AttributeSet(passThroughs.collect {
      case (source, output) if allowed.contains(output) => source
    }).filter { source =>
      !otherReferences.contains(source) &&
        passThroughs.forall { case (s, output) => s.exprId != source.exprId ||
          allowed.contains(output) }
    }

    val newChild = rewrite(project.child, childAllowed)
    if (newProjectList == project.projectList) {
      project.withNewChildren(Seq(newChild))
    } else {
      // Only the encoding of input_file_name() changed, which does not change what the CPU
      // expressions describe, so keep them as they are.
      project.copy(projectList = newProjectList, child = newChild)(project.cpuProjectList)
    }
  }

  /** The child column that `expr` passes through to its output unchanged, if it does. */
  private def passThroughSource(expr: NamedExpression): Option[Attribute] = expr match {
    case attr: AttributeReference => Some(attr)
    case GpuAlias(attr: AttributeReference, _) => Some(attr)
    case GpuAlias(passThrough: GpuStringDictionaryPassThrough, _) =>
      passThrough.children match {
        case Seq(attr: AttributeReference) if attr.dataType.isInstanceOf[StringType] => Some(attr)
        case _ => None
      }
    case _ => None
  }
}

/**
 * Decoding of the DICTIONARY32 columns that input_file_name() can produce (see
 * InputFileNameDictionaryRule), for the operators that accept a dictionary only to turn it
 * back into STRING.
 */
object DictionaryDecode {

  def hasDictionary(columns: Seq[GpuColumnVector]): Boolean =
    columns.exists(_.getBase.getType.equals(DType.DICTIONARY32))

  /** Replaces each DICTIONARY32 column with its decoded STRING column. Closes the input. */
  def decodeAndClose(columns: Array[GpuColumnVector]): Array[GpuColumnVector] = {
    if (!hasDictionary(columns)) {
      columns
    } else {
      withResource(columns) { _ =>
        columns.safeMap { column =>
          if (column.getBase.getType.equals(DType.DICTIONARY32)) {
            GpuColumnVector.from(column.getBase.dictionaryDecode(), column.dataType())
          } else {
            column.incRefCount()
          }
        }
      }
    }
  }

  /** Replaces each DICTIONARY32 column of a batch with its decoded STRING column. */
  def decodeAndClose(batch: ColumnarBatch): ColumnarBatch = {
    val columns = GpuColumnVector.extractColumns(batch)
    if (!hasDictionary(columns)) {
      batch
    } else {
      val numRows = batch.numRows()
      val decoded = withResource(batch) { _ =>
        decodeAndClose(columns.map(_.incRefCount()))
      }
      new ColumnarBatch(decoded.toArray[org.apache.spark.sql.vectorized.ColumnVector], numRows)
    }
  }

  /**
   * Table.rowBitCount for a batch that may hold DICTIONARY32 columns, which libcudf's
   * row_bit_count rejects. A dictionary counts as its indices: a gather keeps it a dictionary,
   * so that is what each row adds to the output.
   */
  def rowBitCount(batch: ColumnarBatch): ai.rapids.cudf.ColumnVector = {
    val columns = GpuColumnVector.extractBases(batch)
    if (!columns.exists(_.getType.equals(DType.DICTIONARY32))) {
      withResource(GpuColumnVector.from(batch))(_.rowBitCount())
    } else {
      val counted = columns.safeMap { column =>
        if (!column.getType.equals(DType.DICTIONARY32)) {
          column.incRefCount()
        } else if (column.getNumChildren == 0) {
          // An empty dictionary may have no children.
          ai.rapids.cudf.ColumnVector.fromInts()
        } else {
          withResource(column.getDictionaryIndices)(_.copyToColumnVector())
        }
      }
      withResource(counted) { _ =>
        withResource(new ai.rapids.cudf.Table(counted: _*))(_.rowBitCount())
      }
    }
  }
}
