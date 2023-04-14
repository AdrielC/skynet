package com.overstock.skynet.http

import com.overstock.skynet.http.metrics.{MetricsMiddleware, MetricsService}
import com.overstock.skynet.service.config.{Config, SkynetConfig}
import com.overstock.skynet.service.model.{ModelTask, Models, ModelsService}
import org.http4s.server.Router
import org.http4s.{EmptyBody, HttpApp, HttpRoutes}
import sttp.model.StatusCode
import sttp.tapir.AnyEndpoint
import sttp.tapir.server.metrics.{EndpointMetric, Metric}
import sttp.tapir.server.http4s.{Http4sServerInterpreter, Http4sServerOptions}
import sttp.tapir.server.interceptor.metrics.MetricsRequestInterceptor
import sttp.tapir.swagger.{SwaggerUI, SwaggerUIOptions}
import zio.{Task, ZIO, ZLayer}
import zio.interop.catz._
import cats.implicits._
import zio.duration.{Duration}

class Routes(
  skynetConfig: SkynetConfig,
  val endPoints: Endpoints,
  zHttp4sServerOptions: Http4sServerOptions[ModelTask]
) {
  import endPoints._

  private def collectMetrics(e: sttp.tapir.AnyEndpoint, code: StatusCode)
                            (implicit startTime: Duration): ModelTask[Unit] =
    zio.clock.nanoTime >>= (responseTime => {
      val duration = Duration.fromNanos(responseTime).minus(startTime)
      e.info.name.fold[ModelTask[Unit]](ZIO.unit)(
        metricsCollector.collectMetrics(duration, _, code.code))
    })

  private val metricsCollector = MetricsMiddleware()

  private val metricsService = MetricsService()

  private implicit val interpreter: Http4sServerInterpreter[ModelTask] =
    Http4sServerInterpreter[ModelTask](zHttp4sServerOptions.appendInterceptor(
      new MetricsRequestInterceptor[ModelTask](
        metrics = List(
          Metric[ModelTask, Http4sResponseBody[ModelTask]](
            metric = Right(EmptyBody),
            onRequest = (_, _, _) => zio.clock.nanoTime.map(Duration.fromNanos)
              .flatMap(implicit startTime =>
                Task(EndpointMetric[ModelTask](
                  onEndpointRequest = None,
                  onResponseHeaders = Some((e, r) => collectMetrics(e, r.code)),
                  onException = (Some(
                    Function.untupled((_: (AnyEndpoint, Throwable)).leftMap(_.info.name) match {
                      case (Some(name), r: ErrorResponse) => metricsCollector.recordException(name, r.error.some)
                      case (Some(name), _) => metricsCollector.recordException(name)
                      case _ => ZIO.unit
                    })))))))),
        ignoreEndpoints = utilityEndpoints)))

  private val transform = transformEndpoint.toRoute(ModelsService.transform)

  private val rank = rankEndpoint.toRoute(ModelsService.rank)

  private val modelHealthCheckRoute = modelHealthCheck.toRoute((ModelsService.warmupModel _).tupled)

  private val serviceHealthCheckRoute = serviceHealthCheck.toRoute(_ => Models.getModels.unit)

  private val loadModelRoute = loadModelEndpoint.toRoute(Models.put)

  private val unloadModelRoute = unloadModelEndpoint.toRoute(Models.delete)

  private val getModelRoute = getModelEndpoint.toRoute(Models.getModelInfo)

  private val getModelsRoute = getModelsEndpoint.toRoute(_ => Models.getModels.map(_.values.toList))

  private val getModelSampleRoute = getSampleInput.toRoute(ModelsService.getModelSample)

  private val graphModelRoute = modelVisualize.toRoute(ModelsService.graphModel)

  private val metrics: HttpRoutes[ModelTask] = metricsCollector.threadMetrics(metricsService)

  private val docs: HttpRoutes[ModelTask] = interpreter.toRoutes(
    SwaggerUI[ModelTask](
      yaml = docsYaml,
      options = SwaggerUIOptions.default
        .contextPath(skynetConfig.swagger.contextPath)
        .yamlName(skynetConfig.swagger.yamlName)))

   val app: HttpApp[ModelTask] = {

     val serviceRoutes = List(
       rank,
       transform,
       modelHealthCheckRoute,
       serviceHealthCheckRoute,
       loadModelRoute,
       unloadModelRoute,
       getModelRoute,
       getModelsRoute,
       getModelSampleRoute,
       graphModelRoute,
       docs
     ).reduce(_ <+> _)

     Router(
       "/"        -> serviceRoutes,
       "/metrics" -> metrics
     ).orNotFound
   }
}

object Routes {

  val live = ZLayer.fromService((conf: Config.Service) => new Routes(
    conf.config,
    endPoints = new Endpoints(conf.config.model.models.foldK),
    zHttp4sServerOptions = defaultServerOptions
  ))
}
