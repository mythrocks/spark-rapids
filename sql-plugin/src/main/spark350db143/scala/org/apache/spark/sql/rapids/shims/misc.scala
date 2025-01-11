/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ColumnVector
import com.nvidia.spark.rapids.{GpuColumnVector, GpuBinaryExpression, GpuScalar}
import com.nvidia.spark.rapids.Arm.withResource

import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.util.MapData
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

  override def doColumnar(lhs: GpuColumnVector, rhs: GpuScalar): ColumnVector =
    throw new UnsupportedOperationException("CALEB: Fail Vector/Scalar")

  override def doColumnar(lhs: GpuScalar, rhs: GpuColumnVector): ColumnVector = {

    ai.rapids.cudf.TableDebug.get().debug("CALEB: rhs: ", rhs.getBase)

    println("CALEB: Extracting the first row: ")

    val keyValueStructScalar = withResource(rhs.getBase.getScalarElement(0)) { rhsListScalar =>
      withResource(rhsListScalar.getListAsColumnView) { rhsListAsColumn =>
        rhsListAsColumn.getScalarElement(0)
      }
    }

    withResource(keyValueStructScalar) { _ =>
      withResource(keyValueStructScalar.getChildrenFromStructScalar) { childCols =>
        withResource(childCols(0).getScalarElement(0)) { keyScalar =>
          withResource(childCols(1).getScalarElement(0)) { valueScalar =>
            println(s"CALEB: keyScalar: ${keyScalar.isValid}")
            println(s"CALEB: valueScalar: ${valueScalar.isValid}")
          }
        }
      }
    }
//    val rhsFront: Int = null.asInstanceOf[Int]
//    println(s"CALEB: ${if (rhsFront == null) 0 else 1 }")
//    val hostRhs = rhs.copyToHost()
//    val rhsFront = hostRhs.getBase

//    throw new UnsupportedOperationException("CALEB: Fail Scalar/Vector")
    if (rhs.getRowCount <= 0) {
      // For the case: when(condition, raise_error(col("a"))
      return GpuColumnVector.columnVectorFromNull(0, NullType)
    }
    // Take the first one as the error message
    withResource(rhs.getBase.getScalarElement(0)) { scalarMsg =>
      if (!scalarMsg.isValid()) {
        throw new RuntimeException()
      } else {
        throw new RuntimeException(scalarMsg.getJavaString())
      }
    }
  }

  override def doColumnar(numRows: Int, lhs: GpuScalar, rhs: GpuScalar): ColumnVector = {
      val errorClass = lhs.getValue.asInstanceOf[UTF8String]
      val errorParams = rhs.getValue.asInstanceOf[MapData]
      throw raiseError(errorClass, errorParams)
  }
}
