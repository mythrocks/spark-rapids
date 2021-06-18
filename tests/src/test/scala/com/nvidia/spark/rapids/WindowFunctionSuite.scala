/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

class WindowFunctionSuite extends SparkQueryCompareTestSuite {

  private def windowAggregationTesterForDecimal(
    windowSpec: WindowSpec, scale: Int = 0): DataFrame => DataFrame =
    (df: DataFrame) => {
      val decimalType = DecimalType(precision = 18, scale = scale)
      val sumDecimalType = DecimalType(precision = 8, scale = 0)
      df.select(
        col("uid").cast(decimalType),
        col("dollars"),
        col("dateLong").cast(decimalType)
      ).withColumn("decimalDollars", col("dollars").cast(decimalType)
      ).withColumn("sumDecimalDollars", col("dollars").cast(sumDecimalType)
      ).select(
        // col("uid"), 
        // col("dateLong"),
        // col("dollars"),
        sum("sumDecimalDollars").over(windowSpec),
        min("decimalDollars").over(windowSpec),
        max("decimalDollars").over(windowSpec),
        count("decimalDollars").over(windowSpec)
      )/*.orderBy(col("uid"), col("dateLong"), col("dollars"))*/
    }

  testSparkResultsAreEqual("[Window] [ROWS] [UNBOUNDED PRECEDING, 3] ", windowTestDfOrc) {
    val rowsWindow = Window.partitionBy("uid")
      .orderBy("dateLong")
      .rowsBetween(Window.unboundedPreceding, 3)
    windowAggregationTesterForDecimal(rowsWindow, scale = 10)
  }

}
