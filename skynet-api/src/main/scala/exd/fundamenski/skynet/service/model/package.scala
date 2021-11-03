package exd.fundamenski.skynet.service

import exd.fundamenski.skynet.domain.Req.{RankFrame, TransformFrame}
import exd.fundamenski.skynet.service.config.Config
import exd.fundamenski.skynet.domain.{Frame, GetSample, GraphModel, Model, ModelInfo, RankingResult}
import exd.fundamenski.skynet.http.{DurOps, ErrorResponse}
import exd.fundamenski.skynet.service.model.Models.Service.ModelsError.{ModelNotFound, ModelTimeout, NoModelsLoaded}
import exd.fundamenski.skynet.service.threads.Async
import exd.fundamenski.skynet.service.transform.Transform
import exd.fundamenski.skynet.service.repository.Repository
import exd.fundamenski.skynet.util.render.Op
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import zio.{Has, RIO, Task, UIO, ULayer, URIO, ZIO, ZLayer, ZManaged}
import zio.blocking.{Blocking, effectBlocking}
import zio.cache.{Cache, Lookup}
import zio.clock.Clock
import zio.stm.{TMap, ZSTM}

import java.io.File
import java.net.URI
import scala.concurrent.TimeoutException
import zio.duration._

package object model {

  type ModelEnv = Blocking with Clock with Models with Repository with Async with ModelsService with ModelsCache
  type ModelTask[Success] = RIO[ModelEnv, Success]
  type UModelTask[Success] = URIO[ModelEnv, Success]
  type ModelOp[Success] = ZIO[Models with ModelsCache, Throwable, Success]
  type ModelIO[Success] = ZIO[ModelsCache, Throwable, Success]

  case class SavedModel(
    modelInfo: ModelInfo,
    transform: Transform.Service
  )

  type ModelsCache = Has[ModelsCache.Service]
  object ModelsCache {
    import cats.implicits._

    type Env = Blocking with Repository with Clock with Config
    type Service = (Cache[String, Throwable, SavedModel], TMap[String, Model])

    val live = ZLayer.fromServiceM((c: Config.Service) =>
      TMap.fromIterable(c.config.model.models.toList
        .foldMapK[Map[String, *], Model](_.map { case (name, uri) => name -> Model(name, uri) }))
        .commit
        .flatMap(m => Cache.make[String, Env, Throwable, SavedModel](
          capacity    = 5,
          timeToLive  = 30.minutes,
          lookup      = Lookup { k =>
              for {
                request: Model   <- m.get(k).flatMap {
                  case Some(value)  => ZSTM.succeed(value)
                  case None         => m.keys >>= (keys =>
                    ZSTM.fail[Throwable](if (keys.isEmpty) NoModelsLoaded() else ModelNotFound(k, keys)))
                }.commit
                bundle    <- model.loadBundle(request)(1.minute)
                service   <- Transform.Service.InMemoryTransformer(bundle.root)
              } yield {
                val info      = ModelInfo(
                  modelName     = request.modelName,
                  uri           = request.uri,
                  info          = bundle.info,
                  inputSchema   = service.inputSchema,
                  outputSchema  = service.outputSchema)
                SavedModel(info, service)
              }
          }).mapEffect(_ -> m)))
  }

  type ModelsService = Has[ModelService]
  object ModelsService {
    implicit val timeout = 1.minute.asScala.toFinite
    type Task[A] = ZIO[ModelEnv with ModelsService, ErrorResponse, A]
    val live = ZLayer.fromServiceM((config: Config.Service) => ModelService(config.config.model))
    private val S = ZIO.service[ModelService]
    def rank(req: RankFrame): Task[RankingResult] = S.flatMap(_.rank(req)).mapError(ErrorResponse.toServiceError(_))
    def transform(req: TransformFrame): Task[DefaultLeapFrame] = S.flatMap(_.transform(req)).mapError(ErrorResponse.toServiceError(_))
    def loadModels(models: Map[String, URI]): Task[Iterable[Model]] = S.flatMap(_.loadModels(models)).mapError(ErrorResponse.toServiceError(_))
    def warmupModel(name: String, n: Int, nRow: Int = 200): Task[Unit] = S.flatMap(_.warmupModel(name, n, nRow)).mapError(ErrorResponse.toServiceError(_))
    def getModelSample(getSample: GetSample): Task[Frame] = S.flatMap(_.getModelSample(getSample)).mapError(ErrorResponse.toServiceError(_))
    def graphModel(visualizeModel: GraphModel): Task[scala.xml.NodeSeq] = S.flatMap(_.graphModel(visualizeModel)).mapError(ErrorResponse.toServiceError(_))
  }

  type Models = Has[Models.Service]
  object Models {

    def service[A](f: Models.Service => RIO[Models with ModelsCache, A]): ModelOp[A] =
      ZIO.service[Models.Service].flatMap(f)

    val live: ULayer[Models] = ZLayer.succeed(Service.LocalModelsCache)

    val getModels: ModelOp[Map[String, Model]] = service(_.getModels)
    def getModelInfo(name: String): ModelOp[ModelInfo] = service(_.getModelInfo(name))
    def getTransformer(name: String): ModelOp[Transform.Service] = service(_.getTransformer(name))
    def getOp(name: String): ModelOp[Op] = service(_.getOp(name))
    def delete(name: String): ModelOp[Option[ModelInfo]] = service(_.delete(name))
    def put(model: Model): ModelOp[Model] = service(_.put(model))

    trait Service extends Serializable {

      def getModels: ModelIO[Map[String, Model]]

      def put(model: Model): ModelIO[Model]

      def delete(modelName: String): ModelIO[Option[ModelInfo]]

      def getTransformer(name: String): ModelIO[Transform.Service]

      def getOp(name: String): ModelIO[Op] = getTransformer(name).map(_.op)

      def getModelInfo(name: String): ModelIO[ModelInfo]

      def getModel(name: String): ModelIO[Model]

      def listModels: ModelIO[List[String]]
    }

    object Service {

      object LocalModelsCache extends Models.Service {

        override val getModels: ModelIO[Map[String, Model]] =
          ZIO.service[ModelsCache.Service].flatMap(_._2.toMap.commit)

        override def put(model: Model): ModelIO[Model] =
          for {
            (_, keys) <- ZIO.service[ModelsCache.Service]
            _         <- keys.put(model.modelName, model).commit
          } yield model

        override def delete(modelName: String): ModelIO[Option[ModelInfo]] = for {
          (models, svc) <- ZIO.service[ModelsCache.Service]
          model         <- models.get(modelName)
          _             <- models.invalidate(modelName)
          _             <- svc.delete(modelName).commit
        } yield Some(model.modelInfo)

        private def getF[O](name: String, f: SavedModel => O): ModelIO[O] =
          ZIO.service[ModelsCache.Service].flatMap(_._1.get(name).map(f))

        override def getTransformer(name: String): ModelIO[Transform.Service] =
          getF(name, _.transform)

        override def getModel(name: String): ModelIO[Model] =
          getF(name, _.modelInfo.model)

        override def getModelInfo(name: String): ModelIO[ModelInfo] =
          getF(name, _.modelInfo)

        override def listModels: ModelIO[List[String]] =
          ZIO.service[ModelsCache.Service].flatMap(_._2.keys.commit)
      }

      sealed trait ModelsError extends Exception
      object ModelsError {
        final case class ModelTimeout(
          modelName: String,
          timeout: Duration,
          message: String = "please enhance your calm") extends TimeoutException(
          s"Timeout error within ${timeout.render} for model [$modelName]: $message"
        ) with ModelsError

        final case class NoModelsLoaded() extends ModelsError {
          override def getMessage: String = "No models loaded"
        }

        final case class NotAnMleapModel(modelName: String) extends ModelsError {
          override def getMessage: String = s"$modelName is not an inspectable Mleap model"
        }

        final case class ModelNotFound(modelName: String, loadedModels: List[String]) extends ModelsError {
          override def getMessage: String = s"$modelName not found in [${loadedModels.mkString(",")}]"
        }
      }
    }
  }

  def rankOrder(desc: Boolean): Double => Double = if (desc) -_ else identity

  def loadFromBundleFile(file: File): RIO[Blocking, Bundle[Transformer]] =
    ZManaged.fromAutoCloseable(Task(BundleFile(file)))
      .onExit(_ => UIO(file.delete()))
      .use(bundle => effectBlocking(bundle.load[MleapContext, Transformer].get))

  private[model] def loadBundle(model: Model)(implicit timeout: Duration) = {
    (for {
      repo        <- ZIO.service[Repository.Service]
      bundle      <- repo.downloadFromUri(model.uri)
      transformer <- loadFromBundleFile(bundle.toFile)
    } yield transformer).timeoutFail(ModelTimeout(model.modelName, timeout, "bundle load timed out"))(timeout)
  }
}
