/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "350db143"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import ai.rapids.cudf.{ColumnVector, ColumnView, DType, Scalar}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuBinaryExpression, GpuMapUtils, GpuScalar}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.errors.QueryExecutionErrors.raiseError
import org.apache.spark.sql.types.{AbstractDataType, DataType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class GpuRaiseError(left: Expression, right: Expression) extends GpuBinaryExpression with ExpectsInputTypes {

  val errorClass: Expression = left
  val errorParams: Expression = right

  override def dataType: DataType = NullType
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType)
  override def toString: String = s"raise_error($errorClass, $errorParams)"

  /** Could evaluating this expression cause side-effects, such as throwing an exception? */
  override def hasSideEffects: Boolean = true

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuColumnVector): ColumnVector = {
    val input = rhs

    if (input.getRowCount <= 0) {
      // For the case: when(condition, raise_error(col("a"))
      return GpuColumnVector.columnVectorFromNull(0, NullType)
    }

    // Take the first one as the error message
    withResource(input.getBase.getScalarElement(0)) { scalarMsg =>
      if (!scalarMsg.isValid()) {
        throw new RuntimeException()
      } else {
        throw new RuntimeException(scalarMsg.getJavaString())
      }
    }
  }

  private def extractScalaUTF8String(stringScalar: Scalar): UTF8String = {
    if (stringScalar.getType != DType.STRING) {
      throw new UnsupportedOperationException("Unexpected scalar type, Expected String Scalar")
    }

    GpuScalar.extract(stringScalar).asInstanceOf[UTF8String]
  }

  private def extractStrings(stringsColumn: ColumnView): Array[UTF8String] = {
    val size = stringsColumn.getRowCount.asInstanceOf[Int] // Already checked if exceeds threshold.
    val output: Array[UTF8String] = new Array[UTF8String](size)
    for (i <- 0 until size) {
      output(i) = withResource(stringsColumn.getScalarElement(i)) {
        extractScalaUTF8String(_)
      }
    }
    output
  }

  private def makeMapData(listOfStructs: ColumnView): MapData = {
    val THRESHOLD: Int = 10 // We don't expect more than these many, for raise_error.
    val mapSize = listOfStructs.getRowCount

    if (mapSize > THRESHOLD)
      throw new UnsupportedOperationException("Unexpectedly large parameter map")

    val outputKeys: Array[UTF8String] =
      withResource(GpuMapUtils.getKeysAsListView(listOfStructs)) { listOfKeys =>
        withResource(listOfKeys.getChildColumnView(0)) {
          extractStrings(_)
        }
      }

    val outputVals: Array[UTF8String] =
      withResource(GpuMapUtils.getValuesAsListView(listOfStructs)) { listOfVals =>
        withResource(listOfVals.getChildColumnView(0)) {
          extractStrings(_)
        }
      }

    ArrayBasedMapData(outputKeys, outputVals)
  }

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    throw new UnsupportedOperationException("CALEB: Fail Vector/Scalar")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {

    ai.rapids.cudf.TableDebug.get().debug("CALEB: rhs: ", rhs.getBase)

    println("CALEB: Extracting the first row: ")

    val lhsErrorClass = lhs.getValue.asInstanceOf[UTF8String]

    // TODO: Assert that rhs is not empty.
    val rhsMapData = withResource(rhs.getBase.slice(0,1)) { slices =>
      val firstRhsRow = slices(0)
      makeMapData(firstRhsRow)
    }

    throw raiseError(lhsErrorClass, rhsMapData)
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
      val errorClass = lhs.getValue.asInstanceOf[UTF8String]
      val errorParams = rhs.getValue.asInstanceOf[MapData]
      throw raiseError(errorClass, errorParams)
  }
}
