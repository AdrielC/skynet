package ml.combust.mleap.json
package circe

import io.circe.{Codec, Decoder, Encoder, HCursor, Json}
import io.circe.generic.semiauto.{deriveCodec, deriveDecoder}
import ml.combust.mleap.core.types.{BasicType, ListType, ScalarType, TensorType}
import shapeless.{:+:, CNil, HList, Poly, Poly1, Witness}
import shapeless.syntax.singleton.mkSingletonOps
import sttp.tapir.{Schema, SchemaType}
import io.circe.generic.auto._
import sttp.tapir.generic.auto._
import sttp.tapir.generic.SchemaDerivation
import cats.implicits._
import io.circe.Decoder.Result
import ml.combust.mleap.json.circe.StructField.mapStr
import sttp.tapir.apispec.Discriminator

sealed trait StructField {
  import StructField._
  def toMleapStructField: ml.combust.mleap.core.types.StructField =
    this match {
      case BasicTypeField(name, t) =>
        ml.combust.mleap.core.types.StructField(name, ScalarType(t))
      case DataTypeField(name, DataType.ScalarType(_, base, isNullable)) =>
        ml.combust.mleap.core.types.StructField(name, ScalarType(base, isNullable.getOrElse(true)))
      case DataTypeField(name, DataType.ListType(_, base, isNullable)) =>
        ml.combust.mleap.core.types.StructField(name, ListType(base, isNullable.getOrElse(true)))
      case DataTypeField(name, DataType.TensorType(_, base, isNullable, dims)) =>
        ml.combust.mleap.core.types.StructField(name, TensorType(base, dims, isNullable.getOrElse(true)))
    }
}
object StructField {

  object mapStr extends Poly1 {
    implicit def atWitness[A <: Witness.Lt[String]]: Case.Aux[A, String] =
      at[A] { a: A => a.value }
  }

  private val basicTypes = List(
    implicitly[Schema[Witness.`"string"`.T]],
    implicitly[Schema[Witness.`"boolean"`.T]],
    implicitly[Schema[Witness.`"byte"`.T]],
    implicitly[Schema[Witness.`"short"`.T]],
    implicitly[Schema[Witness.`"int"`.T]],
    implicitly[Schema[Witness.`"long"`.T]],
    implicitly[Schema[Witness.`"float"`.T]],
    implicitly[Schema[Witness.`"double"`.T]],
    implicitly[Schema[Witness.`"byte_string"`.T]]
  )

  implicit val basicTypeSchema: Schema[BasicType] =
    Schema[String](SchemaType.SCoproduct(
      basicTypes, None)(a => basicTypes
      .find(_.default.exists(_._1.toString == a))))
      .description(basicTypes
        .flatMap(s => s.name.map(_.fullName).orElse(s.encodedExample.map(_.toString)))
        .foldSmash("Must be one of ['", "', '", "']"))
      .encodedExample(""""double"""")
      .as[BasicType]

  implicit val codecStructField: Codec[StructField] = new Codec[StructField] {

    override def apply(a: StructField): Json = a match {
      case b: BasicTypeField => BasicTypeField.codecbasicTypeField(b)
      case d: DataTypeField => DataTypeField.codecbasicTypeField(d)
    }

    override def apply(c: HCursor): Result[StructField] = {
      BasicTypeField.codecbasicTypeField(c) orElse
        DataTypeField.codecbasicTypeField(c)
    }
  }

  implicit val schemaStructField: Schema[StructField] = Schema.derived

  def apply(s: ml.combust.mleap.core.types.StructField): StructField = s match {

    case ml.combust.mleap.core.types.StructField(name, ScalarType(base, true)) =>
      StructField.BasicTypeField(name, base)

    case ml.combust.mleap.core.types.StructField(name, dataType) =>
      StructField.DataTypeField(name, DataType(dataType))
  }

  def Boolean(name: String): StructField = StructField.BasicTypeField(name, BasicType.Boolean)
  def String(name: String): StructField = StructField.BasicTypeField(name, BasicType.String)
  def Long(name: String): StructField = StructField.BasicTypeField(name, BasicType.Long)
  def Int(name: String): StructField = StructField.BasicTypeField(name, BasicType.Int)
  def Double(name: String): StructField = StructField.BasicTypeField(name, BasicType.Double)

  case class BasicTypeField(name: String, `type`: BasicType) extends StructField
  object BasicTypeField {
    implicit lazy val codecbasicTypeField: Codec[BasicTypeField] = deriveCodec
  }
  case class DataTypeField(name: String, `type`: DataType) extends StructField
  object DataTypeField {
    implicit lazy val codecbasicTypeField: Codec[DataTypeField] = deriveCodec
  }

  sealed trait DataType
  object DataType  {

    implicit val dataTypeCodec: Codec[DataType] = new Codec[DataType] {
      override def apply(a: DataType): Json = a match {
        case s: ScalarType => ScalarType.codec(s)
        case s: ListType => ListType.codec(s)
        case s: TensorType => TensorType.codec(s)
      }

      override def apply(c: HCursor): Result[DataType] = {
        ScalarType.codec(c) orElse
          ListType.codec(c) orElse
          TensorType.codec(c)
      }
    }

    case class ScalarType(`type`: Witness.`"basic"`.T,
                          base: BasicType,
                          isNullable: Option[Boolean] = None) extends DataType
    object ScalarType {
      implicit lazy val codec: Codec[ScalarType] = deriveCodec
    }

    case class ListType(`type`: Witness.`"list"`.T,
                        base: BasicType,
                        isNullable: Option[Boolean] = None) extends DataType
    object ListType {
      implicit lazy val codec: Codec[ListType] = deriveCodec
    }

    case class TensorType(`type`: Witness.`"tensor"`.T,
                          base: BasicType,
                          isNullable: Option[Boolean] = None,
                          dimensions: Option[Seq[Int]] = None) extends DataType

    object TensorType {
      implicit lazy val codec: Codec[TensorType] = deriveCodec
    }

    implicit lazy val schemaDataType: Schema[StructField.DataType] = Schema.derived

    def apply(dataTypeFormat: ml.combust.mleap.core.types.DataType): StructField.DataType = dataTypeFormat match {
      case ml.combust.mleap.core.types.ScalarType(base, isNullable) =>
        DataType.ScalarType("basic".narrow, base, if(!isNullable) Some(isNullable) else None)
      case ml.combust.mleap.core.types.ListType(base, isNullable) =>
        DataType.ListType("list".narrow, base, if(!isNullable) Some(isNullable) else None)
      case ml.combust.mleap.core.types.TensorType(base, dimensions, isNullable) =>
        DataType.TensorType("tensor".narrow, base, if(!isNullable) Some(isNullable) else None, dimensions)
    }
  }
}