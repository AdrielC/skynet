package com.overstock.skynet.domain

import sttp.tapir.Codec.PlainCodec
import sttp.tapir.DecodeResult.{InvalidValue, Value}
import sttp.tapir.Schema.SName
import sttp.tapir.{CodecFormat, DecodeResult, Schema, ValidationError, Validator}

sealed trait HandleMissing extends Product with Serializable {
  override def toString: String = this match {
    case HandleMissing.Impute() => "impute"
    case HandleMissing.Error() => "error"
  }
}
object HandleMissing {
  case class Impute()  extends HandleMissing
  object Impute {

    implicit val schemaImpute: Schema[Impute] = Schema
      .string[Impute]
      .name(SName("Impute"))
      .encodedExample(Impute().toString)
      .description("Uses the standard imputation value for the corresponding type. For example, " +
        "numerical types are imputed using zero, collection types with empty collections, etc.")

  }
  case class Error()   extends HandleMissing
  object Error {
    implicit val schemaError: Schema[Error] = Schema
      .string[Error]
      .name(SName("Error"))
      .encodedExample(Error().toString)
      .description("Errors out of any required columns are missing")
  }

  private val descr = s"must be one of ['$Impute', '$Error']"

  implicit val schemaHandleMissing: Schema[HandleMissing] = Schema
    .derived
    .validate(Validator.enumeration(List(Impute(), Error())))
    .default(Impute(), Some(Impute().toString))
    .encodedExample(Error().toString)
    .description("Specifies how to handle cases where expected columns are missing from a LeapFrame.")

  implicit val plainCodecHandleMissing: PlainCodec[HandleMissing] = new PlainCodec[HandleMissing] {

    override def rawDecode(l: String): DecodeResult[HandleMissing] = l match {
      case "impute" => Value(Impute())
      case "error"  => Value(Error())
      case other    => InvalidValue(List(ValidationError.Custom(invalidValue = other, message = descr)))
    }

    override def encode(h: HandleMissing): String = h.toString

    val schema: Schema[HandleMissing] = schemaHandleMissing

    override val format: CodecFormat.TextPlain = CodecFormat.TextPlain()
  }
}
