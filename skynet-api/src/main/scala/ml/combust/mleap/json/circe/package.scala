package ml.combust.mleap.json

import exd.fundamenski.skynet.http.DurOps
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, HCursor, Json}
import shapeless.Witness
import cats.implicits._
import io.circe.Decoder.Result
import io.circe.generic.semiauto.deriveCodec
import ml.combust.bundle.dsl.BundleInfo
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.executor.{BundleMeta, LoadModelRequest, Model, ModelConfig}
import scalapb.{GeneratedMessage, GeneratedMessageCompanion, Message}
import sttp.tapir.Schema
import io.circe.syntax._
import ml.combust.mleap.core.types
import ml.combust.mleap.core.types.{BasicType, DataType, ListType, ScalarType, StructType, TensorType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, LeapFrame, Row}
import ml.combust.mleap.tensor.{ByteString, DenseTensor, SparseTensor, Tensor}
import spray.json.JsonFormat

import java.net.URI
import java.util.Base64
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.control.NonFatal


package object circe {

  object SprayConverters {

    implicit def jsonFormatToCodec[T](implicit J: JsonFormat[T]): Codec[T] = new Codec[T] {
      import spray.json._

      override def apply(c: HCursor): Result[T] = Try {
        c.value.noSpaces.parseJson.convertTo[T]
      }.toEither.leftMap(DecodingFailure.fromThrowable(_, c.history))

      override def apply(a: T): Json = io.circe.parser.parse(a.toJson.prettyPrint).right.get
    }
  }


  implicit def codecWitness[T <: String](implicit W: Witness.Aux[T]): Codec[T] = Codec.from(
    Decoder[String].emap { string =>
      if (string == W.value) W.value.asRight else Left(s"Invalid value: $string. Must be ${W.value}")
    },
    Encoder.encodeString.contramap[T](str => str)
  )

  implicit def schemaWitness[T <: String](implicit W: Witness.Aux[T]): Schema[T] = Schema.schemaForString
    .map { string => if (string == W.value) W.value.some else None }(str => str)
    .encodedExample(W.value)
    .description(s"Value must be '${W.value}'")


  implicit lazy val uriCodec: Codec[URI] =
    Codec.from(
      Decoder.decodeString.emapTry(s => Try(new URI(s))),
      Encoder.encodeString.contramap(_.toString)
    )

  implicit lazy val finiteDurationJsonFormat: Codec[FiniteDuration] =
    Codec.from(
      Decoder.decodeString.emapTry(s => Try(Duration(s).toFinite)) or
        Decoder.decodeLong.emapTry(s => Try(Duration.fromNanos(s).toFinite)),
      Encoder.instance(obj => Duration.fromNanos(obj.toNanos).toString().asJson)
    )

  implicit lazy val jsonFormatSerializationFormat: Codec[SerializationFormat] =
    Codec.from(
      Decoder.decodeString.emap{
        case "json" => SerializationFormat.Json.asRight
        case "proto" => SerializationFormat.Protobuf.asRight
        case other => s"Invalid SerializationFormat: $other".asLeft
      },
      Encoder.instance(_.toString.asJson)
    )

  implicit def mleapTensorCodec[T: Decoder: Encoder: ClassTag]: Codec[Tensor[T]] =
    Codec.from(
      Decoder.forProduct3[Tensor[T], Option[Seq[Seq[Int]]], Seq[Int], Array[T]](
        "indices", "dimensions", "values") {
        case (indices, dims, values) => Tensor.create(
          values = values,
          dimensions = dims,
          indices = indices
        )
      } or Decoder[Array[T]].map(DenseTensor(_, Seq())),
      Encoder.instance {
        case DenseTensor(values, Seq()) => values.asJson
        case DenseTensor(values, dimensions) => Json.fromFields(Map(
          "dimensions" -> dimensions.asJson,
          "values" -> values.asJson
        ))
        case SparseTensor(indices, values, Seq()) => Json.fromFields(Map(
          "indices" -> indices.asJson,
          "values" -> values.asJson
        ))
        case SparseTensor(indices, values, dimensions) => Json.fromFields(Map(
          "dimensions" -> dimensions.asJson,
          "indices" -> indices.asJson,
          "values" -> values.asJson
        ))
      }
    )


  implicit lazy val BundleByteStringCodec: Codec[ByteString] =
    Codec.from(
      Decoder.decodeString.emapTry(b64 => Try(ByteString(Base64.getDecoder.decode(b64)))),
      Encoder.instance(obj => Json.fromString(Base64.getEncoder.encodeToString(obj.bytes)))
    )

  implicit lazy val MleapBasicTypeCodec: Codec[BasicType] =
    Codec.from(
      Decoder.decodeString.emap {
        case "boolean" => BasicType.Boolean.asRight
        case "byte" => BasicType.Byte.asRight
        case "short" => BasicType.Short.asRight
        case "int" => BasicType.Int.asRight
        case "long" => BasicType.Long.asRight
        case "float" => BasicType.Float.asRight
        case "double" => BasicType.Double.asRight
        case "string" => BasicType.String.asRight
        case "byte_string" => BasicType.ByteString.asRight
        case json => s"invalid basic type: $json".asLeft
      },
      Encoder.encodeString.contramap(_.toString)
    )

  implicit lazy val MleapListTypeCodec: Codec[ListType] = {
    val basicTypeStr = "list"
    Codec.from(
      Decoder.instance { h =>
        val isNullable = h.get[Boolean]("isNullable").forall(identity)
        for {
          _ <- h.get[String]("type")
            .filterOrElse(
              _ == basicTypeStr,
              DecodingFailure(s"value for field 'type' must be '$basicTypeStr'", h.history)
            )
          base <- h.get[BasicType]("base")
        } yield ListType(base, isNullable)
      },
      Encoder.instance { case ListType(base, isNullable) =>
        val requiredFields = Map("type" -> basicTypeStr.asJson, "base" -> base.asJson)
        val fields = if (!isNullable) requiredFields + ("isNullable" -> isNullable.asJson) else requiredFields
        Json.fromFields(fields)
      }
    )
  }

  implicit lazy val MleapTensorTypeCodec: Codec[TensorType] = {
    val basicTypeStr = "tensor"
    Codec.from(
      Decoder.instance { h =>
        for {
          _ <- h.get[String]("type")
            .filterOrElse(
              _ == basicTypeStr,
              DecodingFailure(s"value for field 'type' must be '$basicTypeStr'", h.history)
            )
          base <- h.get[BasicType]("base")
          dimensions <- h.get[Option[Seq[Int]]]("dimensions")
          nullable <- h.get[Option[Boolean]]("isNullable")
        } yield TensorType(base, dimensions, nullable.getOrElse(true))
      },
      Encoder.instance { case TensorType(base, dims, isNullable) =>
        val requiredFields = Map("type" -> basicTypeStr.asJson, "base" -> base.asJson)
        val fields = if (!isNullable) requiredFields + ("isNullable" -> isNullable.asJson) else requiredFields
        dims.fold(fields)(dims => fields + ("dimensions" -> dims.asJson))
        Json.fromFields(fields)
      }
    )
  }

  implicit lazy val MleapScalarTypeCodec: Codec[ScalarType] = {
    val basicTypeStr = "basic"
    Codec.from(
      Decoder.instance { h =>
        for {
          _ <- h.get[String]("type")
            .filterOrElse(
              _ == basicTypeStr,
              DecodingFailure(s"value for field 'type' must be '$basicTypeStr'", h.history)
            )
            base    <- h.get[BasicType]("base")
          nullable  <- h.get[Option[Boolean]]("isNullable")
        } yield {
          ScalarType(base, nullable.getOrElse(true))
        }
      } or Decoder[BasicType].map(ScalarType(_)),
      Encoder.instance {
        case ScalarType(base, true) => base.asJson
        case ScalarType(base, false) => Json.fromFields(Map(
          "type"        -> basicTypeStr.asJson,
          "base"        -> base.asJson,
          "isNullable"  -> false.asJson
        ))
      }
    )
  }


  implicit lazy val MleapStructFieldCodec: Codec[types.StructField] =
    new Codec[types.StructField] {

      override def apply(a: types.StructField): Json =
        StructField(a).asJson

      override def apply(c: HCursor): Result[types.StructField] =
        c.as[StructField].map(_.toMleapStructField)
    }

  implicit lazy val MleapStructTypeCodec: Codec[ml.combust.mleap.core.types.StructType] =
    new Codec[StructType] {

      override def apply(a: StructType): Json = Json.fromFields(Map("fields" -> a.fields.asJson))

      override def apply(c: HCursor): Result[StructType] =
        c.get[Seq[ml.combust.mleap.core.types.StructField]]("fields")
          .flatMap(f => StructType(f)
            .toEither
            .leftMap(DecodingFailure.fromThrowable(_, c.history)))
    }

  implicit def MleapLeapFrameWriterEncoder[LF <: LeapFrame[LF]]: Encoder[LF] =
    Encoder.instance { obj =>
      val schema = obj.schema
      implicit lazy val rowCodec: Encoder[Row] = RowCodec(schema)
      val rows = obj.collect().asJson
      Json.fromFields(Map(
        "schema" -> schema.asJson,
        "rows" -> rows
      ))
    }

  implicit lazy val MleapDefaultLeapFrameReaderCodec: Codec[DefaultLeapFrame] = Codec.from(
    Decoder.instance(h =>
        for {
          schema <- h.get[StructType]("schema")(MleapStructTypeCodec)
          rowDecoder = Decoder.decodeVector(RowCodec(schema))
          rows <- h.get[Vector[Row]]("rows")(rowDecoder)
        } yield DefaultLeapFrame(schema, rows)),
    MleapLeapFrameWriterEncoder[DefaultLeapFrame])

  implicit lazy val codecModelConfig: Codec[ModelConfig] = deriveCodec
  implicit lazy val codecModel: Codec[Model] = deriveCodec
  import exd.fundamenski.skynet.util.json.ProtobufConverters._
  implicit lazy val codecBundleInfo: Codec[BundleInfo] = deriveCodec
  implicit lazy val codecBundleMeta: Codec[BundleMeta] = deriveCodec
  implicit lazy val codecLoadModelRequest: Codec[LoadModelRequest] = deriveCodec
}
