package com.nvidia.spark.rapids.shims.spark311db

import com.nvidia.spark.rapids.{BaseExprMeta, DataFromReplacementRule, GpuExec, GpuOverrides, GpuWindowExec, RapidsConf, RapidsMeta, SparkPlanMeta}
import com.databricks.sql.execution.window.RunningWindowFunctionExec

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, SortOrder}

class GpuRunningWindowExecMeta(runningWindowFunctionExec: RunningWindowFunctionExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[RunningWindowFunctionExec](runningWindowFunctionExec, conf, parent, rule) {

  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    runningWindowFunctionExec.windowExpressionList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    runningWindowFunctionExec.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    runningWindowFunctionExec.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def tagPlanForGpu(): Unit = {
    try {
//      val winExpr = runningWindowFunctionExec.windowExpressionList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
      println("WinExprs: " + windowExpressions)
//      val partSpec = runningWindowFunctionExec.partitionSpec
      println("PartitionSpec: " + partitionSpec)
//      val orderSpec = runningWindowFunctionExec.orderSpec
      println("OrderSpec: " + orderSpec)
//      val childPlan = childPlans.head.convertIfNeeded
      println("ChildPlan: " + childPlans.head)

    }
    catch
    {
      case ex: Exception => println("Unexpected exception " + ex.getMessage)
    }

    windowExpressions.map(meta => meta.wrapped)
        .filter(expr => !expr.isInstanceOf[NamedExpression])
        .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing functions; " +
            "cannot convert for GPU execution. " +
            "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))
  }

  override def convertToGpu(): GpuExec = {
    GpuWindowExec(
      windowExpressions.map(_.convertToGpu()),
      partitionSpec.map(_.convertToGpu()),
      orderSpec.map(_.convertToGpu().asInstanceOf[SortOrder]),
      childPlans.head.convertIfNeeded(),
      true
    )
  }
}

