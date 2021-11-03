package exd.fundamenski.skynet.domain

import cats.implicits._

sealed trait Req {
  def modelName: String
  def topK: Option[Int]
  def select: Option[Select]
  def rankingColumn: Option[Select]
  def descending: Boolean
  def exec: Option[ExecStrategy]
  def missing: HandleMissing
  def frame: Frame
}

object Req {

  final case class TransformFrame(modelName: String,
                                  topK: Option[Int] = None,
                                  select: Option[Select] = None,
                                  rankingColumn: Option[Select] = None,
                                  descending: Boolean = true,
                                  exec: Option[ExecStrategy] = None,
                                  missing: HandleMissing = HandleMissing.Error(),
                                  frame: Frame) extends Req

  final case class RankFrame(modelName: String,
                             topK: Option[Int] = None,
                             idCol: String,
                             rankCol: Select = Select.field("prediction"),
                             descending: Boolean = true,
                             group: Option[String] = None,
                             avg: Boolean = false,
                             exec: Option[ExecStrategy] = None,
                             missing: HandleMissing = HandleMissing.Error(),
                             frame: Frame) extends Req {

    override val rankingColumn: Option[Select] = None

    lazy val select: Option[Select] =
      (Select.field(idCol) |+| rankCol).some |+| group.map(Select.field)
  }
}

