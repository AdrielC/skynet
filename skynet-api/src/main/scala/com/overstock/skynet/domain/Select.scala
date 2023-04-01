package com.overstock.skynet.domain

import cats.Order
import cats.data.NonEmptyList
import cats.implicits._
import cats.kernel.Semigroup
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.Row
import sttp.tapir.Codec.PlainCodec
import sttp.tapir.Schema.SName
import sttp.tapir.Schema.annotations.{encodedExample, encodedName, format}
import sttp.tapir.{CodecFormat, DecodeResult, Schema, Validator}

import scala.util.Try

sealed trait Select {
  import Select._

  def apply(schema: StructType): Try[Row => Double] = this match {
    case Field(name)  => schema.indexOf(name).map { idx => (row: Row) => row.getDouble(idx) }
    case Times(l, r)  => (l(schema), r(schema)).mapN((ls, rs) => (row: Row) => ls(row) * rs(row))
    case Many(t)      => t.map(_(schema)).reduceLeft((f, b) => (f, b).mapN((f, b) => (row: Row) => f(row) * b(row)))
  }

  def fields: NonEmptyList[String] = (this match {
    case Field(name)        => NonEmptyList.one(name)
    case Times(left, right) => left.fields concatNel right.fields
    case Many(selects)      => NonEmptyList.fromListUnsafe(selects)
      .flatMap(_.fields.flatMap(s => NonEmptyList.fromListUnsafe(s.split(",").toList)))
  }).distinct

  override def toString: String = this match {
    case Field(name) => name
    case Many(selects) => selects.mkString(",")
    case Times(left, right) => s"$left*$right"
  }
}
object Select {
  import cats.implicits._

  def field(string: String): Select = many(string)

  def many(field: String, fields: String*): Select =
    Many(field, fields: _*)

  case class Field(name: String) extends Select
  object Field {
    implicit val fieldSchema: Schema[Field] = Schema
      .string[Field]
      .name(SName("Field"))
      .description("Select a single field")
      .encodedExample("result_sku_id")
  }

  case class Many(selects: List[Select]) extends Select
  object Many {

    implicit val manySchema: Schema[Many] = Schema
      .string[Many]
      .name(SName("Many"))
      .description("Select multiple comma seperated columns")
      .encodedExample("result_sku_id,context_sku_id,prediction")

    private[Select] def apply(field: String, fields: String*): Select =
      NonEmptyList.fromListUnsafe((field +: fields.toList).distinct.flatMap(s => s.split(",").toList)) match {
        case NonEmptyList(head, Nil) => Select.Field(head)
        case NonEmptyList(head, tl) => Many(Field(head) +: tl.map(Field(_)))
      }
  }

  case class Times private[Select] (left: Select, right: Select) extends Select
  object Times {
    implicit val timesSchema: Schema[Times] = Schema
      .string[Times]
      .name(SName("Times"))
      .encodedExample("prediction*sale_price")
      .description("Select the product of the left and right columns")
      .validate(Validator.pattern("^(.*)\\*(.*)$").contramap(_.toString))
  }

  implicit lazy val schemaSelect: Schema[Select] = Schema
    .derived
    .description("Selects values from rows")

  implicit lazy val stringDecoder: PlainCodec[Select] = new PlainCodec[Select] {

    override def rawDecode(l: String): DecodeResult[Select] = l.split('*').toList match {
      case Nil    => DecodeResult.Missing
      case h :: t => DecodeResult.Value(t.foldLeft(Select.many(h))((s, f) => Times(s, Select.many(f))))
    }
    override def encode(h: Select): String = h match {
      case Field(name)        => name
      case Many(nel)          => nel.mkString(",")
      case Times(left, right) => encode(left) + "*" + encode(right)
    }

    override def schema: Schema[Select] = schemaSelect

    override def format: CodecFormat.TextPlain = CodecFormat.TextPlain()
  }

  implicit val orderSelect: Order[Select] =
    Order.by(_.toString)

  implicit val semigroupSelect: Semigroup[Select] = Semigroup.instance((a, b) =>
    (a, b) match {
      case (Field(a), Field(b))   => Select.many(a, b)
      case (Many(a), Many(b))     => Many((a ++ b).distinct)
      case (Many(a), b)           => Many((a :+ b).distinct)
      case (a, b)                 => Select.Many(List(a, b).distinct)
    }
  )
}
