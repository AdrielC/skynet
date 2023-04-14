package com.overstock.skynet.util.json

import io.circe._
import io.circe.syntax._
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.DecodeResult.Error.{JsonDecodeException, JsonError}
import sttp.tapir.DecodeResult.{Error, Value}
import sttp.tapir.Schema.SName
import sttp.tapir.SchemaType._
import sttp.tapir._

object TapirJsonCirce extends TapirJsonCirce
trait TapirJsonCirce {
  def jsonBody[T: Encoder: Decoder: Schema]: EndpointIO.Body[String, T] = stringBodyUtf8AnyFormat(circeCodec[T])

  lazy val jsonPrinter: Printer = Printer(dropNullValues = true, indent = "")

  implicit def circeCodec[T: Encoder: Decoder: Schema]: JsonCodec[T] =
    sttp.tapir.Codec.json[T] { s =>
      io.circe.parser.decode[T](s) match {
        case Left(failure @ ParsingFailure(msg, _)) =>
          Error(s, JsonDecodeException(List(JsonError(msg, path = List.empty)), failure))
        case Left(failure: DecodingFailure) =>
          val path = CursorOp.opsToPath(failure.history)
          val fields = path.split("\\.").toList.filter(_.nonEmpty).map(FieldName.apply)
          Error(s, JsonDecodeException(List(JsonError(failure.message, fields)), failure))
        case Right(v) => Value(v)
      }
    } { t => jsonPrinter.print(t.asJson) }

  // Json is a coproduct with unknown implementations
  implicit val schemaForCirceJson: Schema[Json] =
    Schema(
      SCoproduct(Nil, None)(_ => None),
      Some(SName("io.circe.Json")))

  implicit val schemaForCirceJsonObject: Schema[JsonObject] =
    Schema(
      SProduct(Nil),
      Some(SName("io.circe.JsonObject")))
}