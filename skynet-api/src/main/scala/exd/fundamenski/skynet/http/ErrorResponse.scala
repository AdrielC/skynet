package exd.fundamenski.skynet.http

import exd.fundamenski.skynet.service.model.Models.Service.ModelsError
import exd.fundamenski.skynet.util.json.jsonBody
import io.circe.generic.semiauto.deriveCodec
import io.circe.{Codec, Decoder, Encoder}
import org.http4s.Status
import sttp.model.StatusCode
import sttp.tapir.{Schema, statusCode}
import sttp.tapir.server.interceptor.ValuedEndpointOutput
import sttp.tapir.server.interceptor.exception.{ExceptionContext, ExceptionHandler}

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
    .encodedExample(StatusCode.BadRequest.code)

  implicit lazy val codecError: Codec[ErrorResponse] = deriveCodec

  implicit lazy val schemaError: Schema[ErrorResponse] = Schema.derived[ErrorResponse]

  def toServiceError(error: Throwable, code: Status = Status.InternalServerError): ErrorResponse =
    ErrorResponse(
      error = Option(error.getClass.getName).getOrElse("java.lang.Throwable"),
      message = Option(error.getMessage).getOrElse("Internal service error"),
      code = StatusCode.unsafeApply(code.code))

  object SkynetExceptionHandler extends ExceptionHandler {

    override def apply(ctx: ExceptionContext): Option[ValuedEndpointOutput[_]] = {
      val e = defaultErrorMapping(ctx.e)
      Some(ValuedEndpointOutput(statusCode.and(jsonBody[ErrorResponse]), (e.code, e)))
    }

    private val defaultErrorMapping: Throwable => ErrorResponse = {
      case timeout: TimeoutException  => toServiceError(timeout, Status.RequestTimeout)
      case m: ModelsError             => toServiceError(m, Status.NotFound)
      case other: Throwable           => toServiceError(other)
    }
  }
}
