package com.overstock.skynet.http.metrics

import com.overstock.skynet.service.model.ModelTask
import cats.data.OptionT

import java.io.{StringWriter, Writer}
import java.util
import fs2.text.{utf8, utf8Encode}
import io.prometheus.client.Collector.MetricFamilySamples
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import org.http4s.{Entity, EntityEncoder, Headers, HttpRoutes, Response}
import org.http4s.headers.`Content-Type`
import zio.Task


object MetricsService {

  implicit def entityWriter[F[_]]: EntityEncoder[F, util.Enumeration[MetricFamilySamples]] =
    new EntityEncoder[F, util.Enumeration[MetricFamilySamples]] {

      override def headers: Headers = Headers(`Content-Type`.parse(TextFormat.CONTENT_TYPE_004).toTry.get)

      override def toEntity(metrics: util.Enumeration[MetricFamilySamples]): Entity[F] = {
        val writer: Writer = new StringWriter()
        TextFormat.write004(writer, metrics)
        Entity(fs2.Stream(writer.toString).through(utf8.encode))
      }
  }

  def apply(registry: CollectorRegistry = CollectorRegistry.defaultRegistry): HttpRoutes[ModelTask] =
    HttpRoutes.liftF[ModelTask] {
      OptionT(Task(Option(Response[ModelTask](body =
        entityWriter[ModelTask].toEntity(registry.metricFamilySamples()).body))))
    }
}