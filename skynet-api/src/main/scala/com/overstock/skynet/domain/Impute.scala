package com.overstock.skynet.domain

import cats.kernel.Monoid
import ml.combust.mleap.tensor.{ByteString, Tensor}

import scala.reflect.ClassTag

trait Impute[T] {
  def impute: T
}

object Impute {
  def apply[A](implicit M: Impute[A]): Impute[A] = M
  def instance[A](_impute: A): Impute[A] = new Impute[A] { val impute: A = _impute }
  def impute[A](implicit M: Impute[A]): A = M.impute

  implicit def fromMonoid[A](implicit M: Monoid[A]): Impute[A] = instance(M.empty)

  implicit def imputeSeq[A]: Impute[Seq[A]] = instance(Nil)

  implicit val imputeBoolean: Impute[Boolean] = instance(false)

  implicit val imputeByteString: Impute[ByteString] = instance(ByteString("".getBytes))

  implicit def imputeTensor[A: ClassTag](implicit dims: Dims): Impute[Tensor[A]] =
    instance(Tensor.create(Array.empty[A], dims.dims.getOrElse(Nil)))

  case class Dims(dims: Option[Seq[Int]] = None)
}
