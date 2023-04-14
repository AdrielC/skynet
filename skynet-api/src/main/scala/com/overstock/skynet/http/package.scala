package com.overstock.skynet

import scala.concurrent.duration._
import spray.json._
import DefaultJsonProtocol._
import com.overstock.skynet.http.ErrorResponse.SkynetExceptionHandler
import com.overstock.skynet.service.model.{ModelEnv, ModelTask}
import fs2.Pipe
import org.http4s.{EntityBody, HttpRoutes}
import org.http4s.websocket.WebSocketFrame
import sttp.capabilities.fs2.Fs2Streams
import sttp.tapir.Codec.{PlainCodec, XmlCodec}
import sttp.tapir.CodecFormat.Xml
import sttp.tapir.{DecodeResult, Endpoint}
import sttp.tapir.server.ServerEndpoint
import sttp.tapir.server.http4s.Http4sServerOptions.{defaultCreateFile, defaultDeleteFile}
import sttp.tapir.server.http4s.{Http4sServerInterpreter, Http4sServerOptions}
import sttp.tapir.server.interceptor.decodefailure.{DecodeFailureInterceptor, DefaultDecodeFailureHandler}
import sttp.tapir.server.interceptor.exception.ExceptionInterceptor
import zio.{RIO, Task, ZIO}

import scala.io.Source
import scala.util.Try
import scala.xml.NodeSeq
import sttp.tapir.ztapir._
import zio.blocking.Blocking

import java.net.URI

package object http extends ZTapir {

  import org.http4s.circe.CirceEntityCodec._
  import org.http4s.circe._

  type Http4sResponseBody[F[_]] = Either[F[Pipe[F, WebSocketFrame, WebSocketFrame]], EntityBody[F]]

  implicit class TryWrapper[A](`try`: => Try[A]) {
    def runTask: Task[A] = ZIO.fromTry(`try`)
    def runBlockingTask: RIO[Blocking, A] = zio.blocking.blocking(ZIO.fromTry(`try`))
  }

  implicit class TaskWrapper[R, E, A](z: => ZIO[R, E, A]) {
    def blocking: ZIO[R with Blocking, E, A] = zio.blocking.blocking(z)
  }

  implicit class DurOps(private val duration: Duration) extends AnyVal {
    def toFinite: FiniteDuration = FiniteDuration(duration.length, duration.unit)
    def toZIO: zio.duration.Duration = zio.duration.Duration.fromScala(duration)
  }

  implicit val myXmlCodec: XmlCodec[NodeSeq] = implicitly[PlainCodec[String]]
    .map(s => scala.xml.parsing.XhtmlParser(Source.fromString(s)))(_.toString()).format(Xml())

  implicit val plainCodecURI: PlainCodec[URI] = implicitly[PlainCodec[String]]
    .mapDecode(s => Try(new URI(s)).fold(DecodeResult.Error(s, _), DecodeResult.Value(_)))(_.toString)
    .schema(util.json.schemaURI)

  implicit class EndpointToRoute[IN, R, E, O, RR]
  (private val end: Endpoint[IN, R, E, O, Any]) extends AnyVal {
    def toRoute(logic: R => RIO[ModelEnv, O])
               (implicit interpreter: Http4sServerInterpreter[ModelTask],
                ev: IN =:= Unit, ev2: ErrorResponse <:< E): HttpRoutes[ModelTask] = {
      val e = end.zServerLogic(u => (logic(u).mapError(ErrorResponse.toServiceError(_))))
      interpreter.toRoutes(e)
    }
  }

  val defaultServerOptions: Http4sServerOptions[ModelTask] = {
    import zio.interop.catz._
    Http4sServerOptions[ModelTask](
      defaultCreateFile[ModelTask],
      defaultDeleteFile[ModelTask],
      ioChunkSize = 8192 * 2,
      interceptors = List(
        new ExceptionInterceptor(SkynetExceptionHandler()),
        new DecodeFailureInterceptor(DefaultDecodeFailureHandler.default)
      ))
  }
}
