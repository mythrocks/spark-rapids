package com.databricks.sql.transaction.tahoe.rapids

import com.databricks.sql.transaction.tahoe.actions.Action
import com.databricks.sql.transaction.tahoe.hooks.PostCommitHook
import com.databricks.sql.transaction.tahoe.metering.DeltaLogging
import com.databricks.sql.transaction.tahoe._

import org.apache.spark.sql.SparkSession

object GpuDoAutoCompaction extends PostCommitHook
    with DeltaLogging
    with Serializable {
  override val name: String = "GpuDoAutoCompaction"

  override def run(spark: SparkSession,
                   txn: OptimisticTransactionImpl,
                   committedVersion: Long,
                   postCommitSnapshot: Snapshot,
                   committedActions: Seq[Action]): Unit = {
//    new Exception("CALEB: GpuDoAutoCompaction::run()!").printStackTrace()
    println(s"CALEB: txn type: ${txn.getClass}")
    val newTxn = txn.deltaLog.startTransaction()
    new GpuOptimizeExecutor(spark, newTxn, Seq.empty, Seq.empty, committedActions).optimize()
  }

  override def handleError(error: Throwable, version: Long): Unit =
    throw DeltaErrors.postCommitHookFailedException(this, version, name, error)
}