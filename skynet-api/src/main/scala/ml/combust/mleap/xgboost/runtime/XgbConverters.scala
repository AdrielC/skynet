package ml.combust.mleap.xgboost.runtime

import biz.k11i.xgboost.util.FVec
import ml.combust.mleap.tensor.{DenseTensor, SparseTensor, Tensor}
import ml.combust.mleap.xgboost.runtime.struct.FVecFactory
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}


trait XgbConverters {
  implicit class VectorOps(vector: Vector) {

    def asXGBPredictor: FVec = {
      vector match {
        case sparseVector: SparseVector =>
          FVecFactory.fromSparseVector(sparseVector)
        case denseVector: DenseVector =>
          FVecFactory.fromDenseVector(denseVector)
      }
    }
  }

  implicit class DoubleTensorOps(tensor: Tensor[Double]) {

    def asXGBPredictor: FVec = {
      tensor match {
        case sparseTensor: SparseTensor[Double] =>
          FVecFactory.fromSparseTensor(sparseTensor)

        case denseTensor: DenseTensor[Double] =>
          FVecFactory.fromDenseTensor(denseTensor)
      }
    }
  }
}

object XgbConverters extends XgbConverters
