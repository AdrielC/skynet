package com.overstock.skynet.util

import com.overstock.skynet.http.DurOps
import com.overstock.skynet.util.json.mleap.RowValue
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.core.types.{BasicType, StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import sttp.tapir.{FieldName, Schema}
import sttp.tapir.SchemaType.{SCoproduct, SProduct, SProductField}

import java.net.URI
import scala.concurrent.duration.{Duration, FiniteDuration}
import cats.implicits._
import com.overstock.skynet.domain.Frame.sample
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax.EncoderOps
import ml.combust.mleap.tensor.{ByteString, Tensor}

import java.util.Base64
import scala.util.Try
import ml.combust.mleap.json.circe._
import shapeless.Witness
import sttp.tapir.Schema.SName

package object json extends TapirJsonCirce {

  implicit def schemaWitness[T <: String](implicit W: Witness.Aux[T]): Schema[T] = Schema.schemaForString
    .map { string => if (string == W.value) Some(W.value) else None }(str => str)
    .encodedExample(W.value)
    .description(s"Value must be '${W.value}'")

  object mleap {

    sealed trait RowValue {
      def rawValue: Any
    }
    object RowValue {

      def apply(any: Any): Option[RowValue] = {
        any match {
          case t: Tensor[_] => Try(TensorValue(t.mapValues(Basic(_).get))).toOption
          case t: Array[_] => t.toList.traverse(Basic(_)).map(l => ListValue(l.toArray))
          case o => Basic(o).map(BasicValue(_))
        }
      }

      implicit val rowValueSchema: Schema[RowValue] = Schema.derived[RowValue]
        .description(
          "The value type within a row at each index is determined by the assiciated StructType " +
            "schema field at the corresponding index")

      case class TensorValue(value: Tensor[Basic]) extends RowValue {
        override val rawValue: Any = value.mapValues(_.value)
      }
      object TensorValue {
        implicit val tensorValueSchema: Schema[TensorValue] =
          Schema.derived[Tensor[Basic]].map(TensorValue(_).some)(_.value)
      }

      case class ListValue(value: Array[Basic]) extends RowValue {
        override val rawValue: Any = value.map(_.value)
      }
      object ListValue {
        implicit val listValueSchema: Schema[ListValue] =
          Basic.basicRowValueSchema.asArray.map(ListValue(_).some)(_.value)
      }

      case class BasicValue(value: Basic) extends RowValue {
        override val rawValue: Any = value.value
      }
      object BasicValue {
        implicit val basicValueSchema: Schema[BasicValue] =
          Basic.basicRowValueSchema.map(BasicValue(_).some)(_.value)
      }

      case class Basic(value: Any)
      object Basic {

        def apply(any: Any): Option[Basic] = {
          any match {
            case v: Boolean => new Basic(v).some
            case v: Byte => new Basic(v).some
            case v: Short => new Basic(v).some
            case v: Int => new Basic(v).some
            case v: Long => new Basic(v).some
            case v: Float => new Basic(v).some
            case v: Double => new Basic(v).some
            case v: String => new Basic(v).some
            case v: ByteString => new Basic(v).some
            case _ => None
          }
        }

        val byteStringSchema = Schema.string[ByteString]
          .description("Base64 encoded byte string")
          .encodedExample(Base64.getEncoder.encodeToString("byte_string".getBytes()))
          .format("base64")

        implicit val basicRowValueSchema: Schema[Basic] = Schema(
          SCoproduct(
            List(
              Schema.schemaForBoolean,
              Schema.schemaForByte,
              Schema.schemaForShort,
              Schema.schemaForInt,
              Schema.schemaForLong,
              Schema.schemaForFloat,
              Schema.schemaForDouble,
              Schema.schemaForString,
              byteStringSchema
            ),
            None
          )(_.value match {
            case _: Boolean => Some(Schema.schemaForBoolean)
            case _: Byte => Some(Schema.schemaForByte)
            case _: Short => Some(Schema.schemaForShort)
            case _: Int => Some(Schema.schemaForInt)
            case _: Long => Some(Schema.schemaForLong)
            case _: Float => Some(Schema.schemaForFloat)
            case _: Double => Some(Schema.schemaForDouble)
            case _: String => Some(Schema.schemaForString)
            case _: ByteString => Some(byteStringSchema)
            case _ => None
          })
        )
      }
    }
  }

  implicit lazy val basicTypeSchema: Schema[BasicType] =
    Schema.string[BasicType]
      .description(List(
        "string",
        "boolean",
        "byte",
        "short",
        "int",
        "long",
        "float",
        "double",
        "byte_string")
        .map(s => s"'$s'")
        .foldSmash("Must be one of [", ", ", "]"))
      .encodedExample("double")

  implicit lazy val structFieldSchema: Schema[StructField] =
    ml.combust.mleap.json.circe.StructField.schemaStructField.map(s => Some(s.toMleapStructField)
    )(ml.combust.mleap.json.circe.StructField(_))

  implicit lazy val schemaRow: Schema[Row] =
    RowValue.rowValueSchema
      .asArray
      .description("any of the valid types specified in the accompanying LeapFrame schema")
      .map(arr => Try(Row(arr.map(_.rawValue): _*)).toOption)(_.map(RowValue(_).get).toArray)

  implicit lazy val structTypeSchema: Schema[StructType] = Schema(
    SProduct(
      List(
        SProductField(
          FieldName("fields"),
          structFieldSchema.asIterable[Seq],
          a => Some(a.fields)))))

  implicit lazy val schemaDefaultLeapFrame: Schema[DefaultLeapFrame] = Schema(
    SProduct[DefaultLeapFrame](
      List(
        SProductField[DefaultLeapFrame, StructType](
          FieldName("schema"),
          structTypeSchema,
          a => Some(a.schema)
        ),
        SProductField[DefaultLeapFrame, Seq[Row]](
          FieldName("dataset", "rows"),
          schemaRow.asIterable[Seq],
          a => Some(a.dataset)
        )
      )
    ),
    name = Some(SName("DefaultLeapFrame"))
  )
    .description("A dataframe of rows with schema. All rows must conform to the schema defined in the 'schema' field")
    .encodedExample(sample.leapFrame.frame.asJson.spaces4)

  implicit lazy val schemaURI: Schema[URI] =
    Schema
      .string[URI]
      .name(SName("URI"))
      .encodedExample("s3://my-bucket/foo/bar.zip")

  implicit lazy val schemaFiniteDuration: Schema[FiniteDuration] =
    Schema
      .string[Duration]
      .description(
        "Parse String into Duration. Format is \"<length><unit>\", " +
          "where whitespace is allowed before, between and after the parts. " +
          "Infinities are designated by \"Inf\", \"PlusInf\", \"+Inf\" and \"-Inf\" or \"MinusInf\".")
      .map(d => Some(d.toFinite))(identity)
      .encodedExample("10 seconds")

  implicit lazy val schemaSerializationFormat: Schema[SerializationFormat] =
    Schema
      .string[SerializationFormat]
      .default(SerializationFormat.Json, Some("json"))

  implicit lazy val schemaByteString: Schema[com.google.protobuf.ByteString] = Schema.schemaForString
    .map(b => Some(com.google.protobuf.ByteString.copyFrom(b.getBytes())))(a => a.toStringUtf8)
}
