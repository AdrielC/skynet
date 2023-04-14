package com.overstock.skynet.http.metrics

import com.overstock.skynet.service.model.ModelTask
import com.overstock.skynet.service.threads.Async
import cats.data.{Kleisli, OptionT}
import io.prometheus.client._
import org.http4s.{HttpRoutes, Status}
import sttp.model.StatusCode
import zio.blocking.{Blocking, blockingExecutor, effectBlocking, effectBlockingIO}
import zio.clock.nanoTime
import zio.duration.Duration
import zio.{RIO, Task, UIO, ZIO}
import zio.internal.Executor

case class MetricsMiddleware(bucketsInMillis: List[Double], registry: CollectorRegistry) {

  def threadMetrics(service: HttpRoutes[ModelTask]): HttpRoutes[ModelTask] =
    Kleisli(req => OptionT(
      for {
        (blocking, async) <- ZIO.services[Blocking.Service, Async.Service]
        _                 <-
          collectExec(blocking.blockingExecutor, "blocking") &>
            collectExec(async.asyncExecutor, "async")
        r                 <- service(req).value
      } yield r
    ))

  def requestMetrics(serviceName: String, service: HttpRoutes[ModelTask]): HttpRoutes[ModelTask] =
    Kleisli(req => OptionT(
      for {
        (duration, rs)        <- service(req).value.timed
        _                     <- (rs match {
          case Some(response) => collectMetrics(duration, serviceName, response.status.code)
          case None           => recordException(serviceName)
        }).forkDaemon
      } yield rs
    ))

  private val totalResponseCounter = Counter
    .build()
    .name("total_reponse_count")
    .help("Total responses")
    .labelNames("service", "status")
    .register(registry)

  private val failedRequestsCounter = Counter
    .build()
    .name("http_requests_total")
    .help("Total http requests received")
    .labelNames("service", "error")
    .register(registry)

  private val poolEnqueued = Gauge
    .build()
    .name("pool_tasks_enqueued")
    .help("The total number of tasks enqueued to the pool")
    .labelNames("pool")
    .register(registry)

  private val poolSize = Gauge
    .build()
    .name("pool_size")
    .help("The number of tasks remaining to be executed in the pool")
    .labelNames("pool")
    .register(registry)

  private val poolWorkersCount = Gauge
    .build()
    .name("workers_count")
    .help("The number of current live worker threads in the pool")
    .labelNames("pool")
    .register(registry)

  private val responseTime = Histogram
    .build()
    .name("http_requests_duration_millis")
    .help("Histogram of the response time of http requests in milliseconds")
    .labelNames("service", "status")
    .buckets(bucketsInMillis: _*)
    .register(registry)

  def collectMetrics(duration: zio.duration.Duration, service: String, code: Int): ModelTask[Unit] =
    effectBlocking(totalResponseCounter.labels(service, code.toString).inc()) &>
      effectBlocking(responseTime.labels(service, code.toString).observe(duration.toMillis))
  def recordException(serviceName: String, error: Option[String] = None): ModelTask[Unit] =
    effectBlocking(failedRequestsCounter.labels(serviceName, error.getOrElse("unchecked")).inc())

  private def collectExec(executor: Executor, name: String): RIO[Blocking, Unit] =
    executor.metrics.fold[RIO[Blocking, Unit]](ZIO.unit) { metrics =>
        val workersCount = metrics.workersCount
        val size = metrics.size
        val enqueued = metrics.enqueuedCount

      effectBlocking(poolWorkersCount.labels(name).set(workersCount)) &>
        effectBlocking(poolSize.labels(name).set(size)) &>
        effectBlocking(poolEnqueued.labels(name).set(enqueued))
    }
}

object MetricsMiddleware {

  val defaultBucketsInMillis: List[Double] = List(
    0.01, 0.025, 0.05, 0.075, 0.085, 0.095, 0.1, 0.125, 0.15, 0.175, 0.2, 0.225, 0.25,
    0.275, 0.3, 0.325, 0.35, 0.4, 0.45, 0.5, 0.6, 0.7, 1, 2, 3, 5, 10)
    .map(_ * 1000)

  def apply(bucketsInMillis: List[Double] = defaultBucketsInMillis,
            registry: CollectorRegistry = CollectorRegistry.defaultRegistry): MetricsMiddleware =
    new MetricsMiddleware(bucketsInMillis, registry)
}