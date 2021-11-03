package exd.fundamenski.skynet

import exd.fundamenski.skynet.service.config.Config
import exd.fundamenski.skynet.service.threads.Async
import exd.fundamenski.skynet.service.model.{ModelEnv, ModelTask}
import exd.fundamenski.skynet.http.Routes
import org.http4s.blaze.channel.{DefaultMaxConnections, DefaultPoolSize}
import org.http4s.server.defaults
import zio.{Has, ZIO}
import zio.interop.catz._
import org.http4s.blaze.server.BlazeServerBuilder

object Server {

  val run = ZIO.runtime[ModelEnv with Config with Has[Routes]].flatMap { implicit runtime =>

    val httpConfig  = runtime.environment.get[Config.Service].config.http
    val asyncEC     = runtime.environment.get[Async.Service].asyncExecutor.asEC
    val routes      = runtime.environment.get[Routes]

    BlazeServerBuilder[ModelTask](asyncEC)
      .bindHttp(port = httpConfig.port, host = httpConfig.host)
      .withHttpApp(routes.app)
      .withMaxConnections(httpConfig.maxConnections.getOrElse(DefaultMaxConnections))
      .withConnectorPoolSize(httpConfig.connectorPoolSize.getOrElse(DefaultPoolSize))
      .withIdleTimeout(httpConfig.idleTimeout.getOrElse(defaults.IdleTimeout))
      .withResponseHeaderTimeout(httpConfig.responseTimeout.getOrElse(defaults.ResponseTimeout))
      .withChunkBufferMaxSize(1024 * 1024 * httpConfig.chunkFactor)
      .withBufferSize(64 * 1024 * httpConfig.chunkFactor)
      .serve
      .compile
      .drain
      .on(asyncEC)
  }
}