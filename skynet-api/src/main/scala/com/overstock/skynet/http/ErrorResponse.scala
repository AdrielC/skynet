package com.overstock.skynet.http

import cats.implicits.none
import com.overstock.skynet.service.model.Models.Service.ModelsError
import com.overstock.skynet.util.json.jsonBody
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder}
import org.http4s.Status
import sttp.model.StatusCode
import sttp.monad.MonadError
import sttp.tapir.{Schema, statusCode}
import sttp.tapir.server.interceptor.exception.{ExceptionContext, ExceptionHandler}
import sttp.tapir.server.model.ValuedEndpointOutput

import scala.concurrent.TimeoutException

final case class ErrorResponse (
  error: String,
  message: String,
  code: StatusCode
) extends Error {

  override def getMessage: String = message
}

object ErrorResponse {

  implicit lazy val codecStatus: Codec[StatusCode] = Codec.from(
    Decoder.decodeInt.emap(StatusCode.safeApply),
    Encoder.encodeInt.contramap(_.code))

  implicit lazy val schemaStatus: Schema[StatusCode] = Schema
    .schemaForInt
    .map(StatusCode.safeApply(_).toOption)(_.code)
    .description("Status code must be in the allowed range of 100-599")
    .encodedExample(StatusCode.InternalServerError.code.toString)
    .copy(default = Some((StatusCode.InternalServerError, none[Any])))

  implicit lazy val codecError: Codec[ErrorResponse] = deriveCodec

  implicit lazy val schemaError: Schema[ErrorResponse] = Schema.derived[ErrorResponse]

  def toServiceError[E <: Throwable](error: E, code: Status = Status.InternalServerError): ErrorResponse =
    ErrorResponse(
      error = Option(error.getClass.getName)
        .map(_.replace(".", "$").split('$').last)
        .getOrElse("Throwable"),
      message = Option(error.getMessage).getOrElse("Internal service error"),
      code = StatusCode.unsafeApply(code.code))


  val defaultErrorMapping: Throwable => ErrorResponse = {
    case timeout: TimeoutException => toServiceError(timeout, Status.RequestTimeout)
    case m: ModelsError => toServiceError(m, Status.NotFound)
    case other: Throwable => toServiceError(other)
  }

  case class SkynetExceptionHandler[F[_]]
  (errorMapping: Function[Throwable, ErrorResponse] = defaultErrorMapping) extends ExceptionHandler[F] {
    def apply(ctx: ExceptionContext)(implicit monad: MonadError[F]): F[Option[ValuedEndpointOutput[_]]] = monad.eval {
      val e = defaultErrorMapping(ctx.e)
      Some(ValuedEndpointOutput(jsonBody[ErrorResponse], e))
    }
  }
}
