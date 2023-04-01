package com.overstock.skynet.http

import com.overstock.skynet.http.metrics.{MetricsMiddleware, MetricsService}
import com.overstock.skynet.service.config.{Config, SkynetConfig}
import com.overstock.skynet.service.model.{ModelTask, Models, ModelsService}
import org.http4s.server.Router
import org.http4s.{EmptyBody, HttpApp, HttpRoutes}
import sttp.model.StatusCode
import sttp.tapir.Endpoint
import sttp.tapir.metrics.{EndpointMetric, Metric}
import sttp.tapir.model.ServerRequest
import sttp.tapir.server.http4s.{Http4sServerInterpreter, Http4sServerOptions}
import sttp.tapir.server.interceptor.metrics.MetricsRequestInterceptor
import sttp.tapir.swagger.http4s.SwaggerHttp4s
import zio.{Task, ZIO, ZLayer}
import zio.interop.catz._
import org.http4s.syntax.kleisli._
import cats.implicits._
import com.overstock.skynet.service.config.SkynetConfig
import com.overstock.skynet.service.config.SkynetConfig

class Routes(
  skynetConfig: SkynetConfig,
  val endPoints: Endpoints,
  zHttp4sServerOptions: Http4sServerOptions[ModelTask, ModelTask]
) {
  import endPoints._

  private val metricsCollector = MetricsMiddleware()

  private val metricsService = MetricsService()

  implicit val interpreter: Http4sServerInterpreter[ModelTask] =
    Http4sServerInterpreter[ModelTask](zHttp4sServerOptions.appendInterceptor(
      new MetricsRequestInterceptor[ModelTask, Http4sResponseBody[ModelTask]](
        metrics = List(
          Metric[ModelTask, Http4sResponseBody[ModelTask]](
            metric = Right(EmptyBody),
            onRequest = (req, _, _) => zio.clock.nanoTime
              .flatMap(startTime =>
                Task(EndpointMetric[ModelTask](
                  onEndpointRequest = None,
                  onResponse = Some((e, r) => collectMetrics(e, r.code)(req, startTime)),
                  onException = Some((e, _) => collectMetrics(e, StatusCode.BadRequest)(req, startTime))))
            ))),
        ignoreEndpoints = utilityEndpoints)))

  val transform =
    transformEndpoint.toRoute(ModelsService.transform)

  val rank =
    rankEndpoint.toRoute(ModelsService.rank)

  val modelHealthCheckRoute =
    modelHealthCheck.toRoute((ModelsService.warmupModel _).tupled)

  val serviceHealthCheckRoute =
    serviceHealthCheck.toRoute(_ => Models.getModels.unit)

  val loadModelRoute =
    loadModelEndpoint.toRoute(Models.put)

  val unloadModelRoute =
    unloadModelEndpoint.toRoute(Models.delete)

  val getModelRoute =
    getModelEndpoint.toRoute(Models.getModelInfo)

  val getModelsRoute =
    getModelsEndpoint.toRoute(_ => Models.getModels.map(_.values.toList))

  val getModelSampleRoute =
    getSampleInput.toRoute(ModelsService.getModelSample)

  val graphModelRoute =
    modelVisualize.toRoute(ModelsService.graphModel)

  private def collectMetrics(e: Endpoint[_, _, _, _], code: StatusCode)
                            (implicit a: ServerRequest, startTime: Long): ModelTask[Unit] =
    zio.clock.nanoTime >>= (responseTime => {
      val duration = zio.duration.Duration.fromNanos(responseTime - startTime)
      e.info.name.fold[ModelTask[Unit]](ZIO.unit)(
        metricsCollector.collectMetrics(duration, code.code, _))
    })

  val docs: HttpRoutes[ModelTask] = new SwaggerHttp4s(
    yaml        = docsYaml,
    contextPath = skynetConfig.swagger.contextPath,
    yamlName    = skynetConfig.swagger.yamlName)
    .routes[ModelTask]

  val metrics: HttpRoutes[ModelTask] =
    metricsCollector.threadMetrics(metricsService)

  val routes =
    List(
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

   val app: HttpApp[ModelTask] = Router(
    "/"         -> routes,
    "/metrics"  -> metrics)
    .orNotFound
}

object Routes {

  val live = ZLayer.fromService((conf: Config.Service) => new Routes(
    conf.config,
    endPoints = new Endpoints(conf.config.model.models.foldK),
    zHttp4sServerOptions = defaultServerOptions
  ))
}
