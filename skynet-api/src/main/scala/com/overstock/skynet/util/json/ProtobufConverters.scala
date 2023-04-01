package com.overstock.skynet.util.json

import io.circe.{Decoder, DecodingFailure, Encoder}
import scalapb.{GeneratedMessageCompanion, Message, GeneratedMessage}

import scala.util.control.NonFatal

trait ProtobufConverters {

  val parser = new scalapb_circe.Parser()
  val printer = new scalapb_circe.Printer(
    includingDefaultValueFields = true,
    formattingLongAsNumber = true
  )

  implicit def protoToDecoder[T <: GeneratedMessage with Message[T]: GeneratedMessageCompanion]: Decoder[T] =
    Decoder.instance { value =>
      try {
        Right(parser.fromJson(value.value))
      } catch {
        case NonFatal(e) =>
          Left(DecodingFailure.fromThrowable(e, value.history))
      }
    }

  implicit def protoToEncoder[T <: GeneratedMessage with Message[T]]: Encoder[T] =
    Encoder.instance(printer.toJson(_))
}

object ProtobufConverters extends ProtobufConverters