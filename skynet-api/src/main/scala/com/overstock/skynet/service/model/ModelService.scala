package com.overstock.skynet.service.model

import com.overstock.skynet.service.config.ModelServiceConfig
import com.overstock.skynet.service.repository.Repository
import com.overstock.skynet.service.transform.Transform
import com.overstock.skynet.domain.{ExecStrategy, _}
import Req._
import com.overstock.skynet.http._
import com.overstock.skynet.service.model.Models.Service.ModelsError.ModelTimeout
import com.overstock.skynet.util.render._
import cats.Order
import cats.data.{NonEmptyChain, NonEmptyList}
import cats.implicits._
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import reftree.diagram.Diagram
import reftree.render.Renderer
import zio.blocking.{Blocking, effectBlocking}
import zio.{RIO, Task, UIO, ZIO, ZManaged}
import cats.implicits._
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.executor.BundleMeta
import org.log4s.{Logger, getLogger}
import zio.cache.{Cache, Lookup}
import zio.clock.Clock

import java.io.File
import java.net.URI
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.util.{Success, Try}
import scala.xml.parsing.XhtmlParser

class ModelService(defaultExec: ExecStrategy) {

  private[this] implicit val logger: Logger = getLogger

  def rank(req: RankFrame): ModelTask[RankingResult] =
    for {

      transformed <- transform(
        modelName = req.modelName,
        frame     = req.frame,
        select    = req.select.map(_.fields),
        exec      = req.exec,
        missing   = req.missing)

      ranked <- rank(
        frame       = transformed,
        descending  = req.descending,
        rankCol     = req.rankCol,
        idCol       = req.idCol,
        avg         = req.avg,
        topK        = req.topK,
        group       = req.group
      ).runBlockingTask

    } yield ranked

  def getModelSample(getSample: GetSample)
                    (implicit timeout: zio.duration.Duration = 30.seconds.toZIO): RIO[ModelEnv, Frame] =
    Models.getModelInfo(getSample.modelName)
      .flatMap { case ModelInfo(_, BundleMeta(_, inputSchema, _)) => effectBlocking {

        def createSimpleFrame: Frame =
          createSampleLeapFrame(StructType(inputSchema.fields.sortBy(_.name)).get, getSample.nRows)

        val nContextRows = if (getSample.nRows <= 0) 0 else 1

        NonEmptyList.fromList(getSample.contextPrefixes)
          .fold[Try[Frame]](Success(createSimpleFrame)) { contextPrefixes =>
            val grouped = inputSchema.fields
              .groupBy(f =>
                contextPrefixes
                  .toList
                  .foldLeft(none[String])((a, b) =>
                    a.orElse(Option(b).filter(_.split('|').foldLeft(false)((p, n) => p | f.name.startsWith(n))))))
              .toList
              .sortBy { case (prefix, _) =>
                val idx = prefix.fold(-1)(getSample.contextPrefixes.indexOf)
                if (idx == -1) Int.MaxValue else idx
              }

            val frame = grouped
              .zipWithIndex
              .map { case ((_, s), i) =>
                createSampleLeapFrame(
                  schema  = StructType(s.sortBy(_.name)).get,
                  n       = if ((i + 1) == grouped.length) getSample.nRows else nContextRows)
              }
              .reduceRightOption[Frame](Frame.CartesianFrame(_, _))
              .getOrElse(createSimpleFrame)

            getSample.select
              .map(_.fields.toList)
              .fold[Try[Frame]](Success(frame))(frame.select(_: _*))
          }.get
      }.timeoutFail(ModelTimeout(getSample.modelName, timeout, "model sample timed out"))(timeout)
    }

  def warmupModel(name: String, n: Int, nRow: Int = 200): RIO[Blocking with Models with ModelsCache, Unit] =
    for {
      model       <- Models.getTransformer(name) <* UIO(logger.info(s"Warming up $name"))
      schema      = model.inputSchema
      _           <- ZIO.collectAll(Stream.fill(n)(model
        .transform(createSampleLeapFrame(schema, nRow).frame, defaultExec).unit))
    } yield logger.info(s"Warmup for $name using $n leap frames completed")

  def transform(modelName: String,
                frame: Frame,
                select: Option[NonEmptyList[String]],
                exec: Option[ExecStrategy],
                missing: HandleMissing): ModelTask[DefaultLeapFrame] =
    for {
      client    <- Models.getTransformer(modelName)
      outFrame  <- client.transform(
        dataset       = frame,
        exec          = exec.getOrElse(defaultExec),
        select        = select,
        handleMissing = missing)
    } yield outFrame

  def transform(req: Req): ModelTask[DefaultLeapFrame] = for {

    transformedFrame <- transform(
      modelName = req.modelName,
      frame     = req.frame,
      select    = req.select.flatMap(_.some |+| req.rankingColumn).map(_.fields),
      exec      = req.exec,
      missing   = req.missing)

    sorted <- (req.rankingColumn match {

      case None => Success(transformedFrame)

      case Some(rankingCol) => rank(
        leapFrame = transformedFrame,
        rankCol   = rankingCol,
        desc      = req.descending,
        topK = req.topK)
        .flatMap { ranked =>

          val rankingCols = rankingCol.fields.toList

          if (req.select.isEmpty || req.select.map(_.fields).exists(_.exists(rankingCols.contains))) {
            Success(ranked)
          } else {
            ranked.drop(rankingCols.filterNot(r => req.select.exists(_.fields.contains_(r))): _*)
          }
        }
    }).runBlockingTask

  } yield sorted

  def graphModel(visualizeModel: GraphModel)
                (implicit timeout: FiniteDuration = 30.seconds)
  : RIO[Clock with Models with ModelsCache, scala.xml.NodeSeq] = {
    for {
      op      <- Models.getOp(visualizeModel.modelName)
      svg     <- Task {
        import visualizeModel._

        val fileOut = os.pwd / visualizeModel.modelName

        val renderer = Renderer(
          renderingOptions  = renderingOptions,
          directory         = fileOut.toNIO,
          format            = "svg")

        renderer.render(modelName, Diagram(op))
        val svg = fileOut / (modelName + ".svg")
        val str = os.read(svg)
        svg.toIO.delete()
        XhtmlParser(scala.io.Source.fromString(str))
      }
        .timeoutFail(new TimeoutException(
          s"Graphing model ${visualizeModel.modelName} timed out in $timeout"))(timeout.toZIO)

    } yield svg
  }

  def loadModels(models: Map[String, URI])(implicit loadTimeout: FiniteDuration)
  : RIO[Blocking with Models with ModelsCache, List[Model]] = {
    logger.info(s"Loading models: $models")
    RIO.collectAll(models.toList.map { case (name, uri) =>
      Models.put(Model(name, uri)) <* warmupModel(name, 200)
    })
  }


  private def rank(leapFrame: DefaultLeapFrame,
                   rankCol: Select,
                   desc: Boolean,
                   topK: Option[Int]): Try[DefaultLeapFrame] =
    for {
      idx         <- rankCol(leapFrame.schema)
      sorted      <- Try(leapFrame.dataset.sortBy(idx andThen rankOrder(desc)))
      topSortedK  = topK.fold(sorted)(sorted.take)
    } yield DefaultLeapFrame(leapFrame.schema, topSortedK)


  private def rank(frame: DefaultLeapFrame,
                   descending: Boolean,
                   rankCol: Select,
                   idCol: String,
                   avg: Boolean,
                   topK: Option[Int],
                   group: Option[String]): Try[RankingResult] = {
    val order = (if (descending) Order[Double] else Order.reverse(Order[Double])).contramap((_: (String, Double))._2)
    for {
      getRank       <- rankCol(frame.schema)
      idColIdx      <- frame.schema.indexOf(idCol)
      rowToIdScore  = (row: Row) => row.getString(idColIdx) -> getRank(row)
      topPerGroup   <- group.fold(frame.dataset.map(rowToIdScore).pure[Try]) { group =>

        frame.schema.indexOf(group).map { groupColIdx =>

          frame.dataset.toList
            .foldMap(r => Map(r.getRaw(groupColIdx) -> NonEmptyChain(rowToIdScore(r))))
            .toSeq
            .map(_._2.minimum(order))
        }
      }
    } yield {

      val withScores = if (avg) {
        topPerGroup
          .toList
          .foldMap { case (id, rank) => Map(id -> (rank, 1.0)) }
          .mapValues { case (sum, count) => sum / count }
      } else {
        topPerGroup
      }

      val sorted = withScores
        .toSeq
        .sorted(order.toOrdering)
        .map { case (id, _) => id }

      RankingResult(topK.fold(sorted)(sorted.take))
    }
  }
}

object ModelService {

  private[this] implicit val logger: Logger = getLogger

  def apply(modelConfig: ModelServiceConfig): RIO[Blocking with Models with ModelsCache, ModelService] = {

    val service = new ModelService(modelConfig.executionStrategy)

    implicit val loadTimeout: FiniteDuration = modelConfig.timeout.getOrElse(1.minute).toFinite

    (modelConfig.models match {

      case None         => UIO(logger.info("No startup models configured, skipping load on startup"))

      case Some(models) => service.loadModels(models)

    }).as(service)
  }
}
