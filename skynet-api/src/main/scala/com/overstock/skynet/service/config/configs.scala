package com.overstock.skynet.service.config

import com.overstock.skynet.default
import com.overstock.skynet.domain.ExecStrategy
import com.overstock.skynet.util.threads
import cats.data.NonEmptyList
import cats.implicits._
import org.log4s.getLogger
import pureconfig.error.{CannotConvert, ConvertFailure}
import pureconfig.ConfigReader
import zio.internal.Executor

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, SynchronousQueue, ThreadPoolExecutor}
import scala.concurrent.duration.{Duration, FiniteDuration, MILLISECONDS}
import scala.util.Try
import pureconfig.generic.semiauto._

import java.net.URI

/** The application configuration
  *
  * @param http The Http settings of the current application
//  * @param auth The Auth configurations of the current application
  * @param swagger The Swagger configurations of the current application
  */
final case class SkynetConfig private[config](
  http: HttpCfg,
  model: ModelServiceConfig,
  swagger: SwaggerCfg
)
object SkynetConfig {

  implicit val configReader: ConfigReader[SkynetConfig] = deriveReader
}

case class ModelServiceConfig private[config](
  repo: RepoConfig,
  executionStrategy: ExecStrategy,
  models: Option[Map[String, URI]],
  warmupRows: Int,
  timeout: Option[Duration],
  executor: ExecutorsConfig
)
object ModelServiceConfig {

  implicit val configReader: ConfigReader[ModelServiceConfig] = deriveReader
}

case class RoutesConfig private[config](
  loadTimeout: Option[Duration],
  transformTimeout: Option[Duration]
)
object RoutesConfig {

  implicit val configReader: ConfigReader[RoutesConfig] = deriveReader
}

case class ExecutorsConfig private[config](
  select: Option[String],
  executors: Map[String, ExecutorConfig]
) {

  private[this] val logger = getLogger

  def create(queue: => BlockingQueue[Runnable]): Option[Executor] =
    select.flatMap(name => executors.get(name).map(_.toExecutor(name, queue)))

  def createBlocking: Executor =
    create(new SynchronousQueue[Runnable](false))
      .getOrElse {
        logger.info("Using default blocking executor")
        default.blockingExecutor
      }

  def createAsync: Executor =
    create(new LinkedBlockingQueue[Runnable])
      .getOrElse {
        logger.info("Using default async executor")
        default.asyncExecutor
      }
}
object ExecutorsConfig {

  implicit val configReader: ConfigReader[ExecutorsConfig] = deriveReader
}

sealed trait RepoConfig extends Product with Serializable
object RepoConfig {
  case object S3                                    extends RepoConfig
  case object File                                  extends RepoConfig
  case object GS                                    extends RepoConfig
  case class Multi(repos: NonEmptyList[RepoConfig]) extends RepoConfig


  implicit lazy val configReader: ConfigReader[RepoConfig] = cur =>
    cur
      .asString
      .toValidated
      .leftMap(_.toNel)
      .andThen {
        case "s3"   => S3.validNel
        case "file" => File.validNel
        case "gs"   => GS.validNel
        case other  => ConvertFailure(CannotConvert(
          value   = other,
          toType  = "RepoConfig",
          because = "must be one of the following: ['s3', 'file', 'gs', '[<repo>]']"
        ), cur).invalidNel
      }
      .findValid {
        cur
          .asList
          .toValidated
          .leftMap(_.toNel)
          .andThen(_
            .traverse(configReader.from(_).toValidatedNel)
            .leftMap(_.flatMap(_.toNel))
            .andThen(NonEmptyList.fromList(_)
              .map(Multi)
              .toValidNel(
                ConvertFailure(CannotConvert(
                  value   = cur.value.render(),
                  toType  = "RepoConfig.Multi",
                  because = "List cannot be empty"), cur))))
      }
      .leftMap(_.toFailures)
      .toEither
}

sealed trait Threads extends Product with Serializable { def n: Int }
object Threads {

  implicit val configReaderNThreads: ConfigReader[Threads] = {
    import pureconfig.error._
    ConfigReader.fromString {
      case N(n) => n.asRight
      case "" => EmptyStringFound("NThreads").asLeft
      case x if x.headOption.exists(_.toLower == 'x') => Try(x.tail).toOption match {
        case Some(X(x)) => x.asRight
        case Some(other) => CannotConvert(other, "NThreads", "string following 'x' must be a double").asLeft
        case None => EmptyStringFound("Int").asLeft
      }
    }
  }

  case class N(n: Int) extends Threads
  object N {
    def unapply(str: String): Option[N] = Try(str.toInt).filter(_ >= 0).map(N(_)).toOption
  }

  case class X(x: Double) extends Threads {
    lazy val n: Int = (Runtime.getRuntime.availableProcessors * x).ceil.toInt
  }
  object X {
    def unapply(str: String): Option[X] = Try(str.toDouble).toOption.filter(_ >= 0.0).map(X(_))
  }
}


case class ExecutorConfig private[config](
  yieldOpCount: Threads,
  corePoolSize: Threads,
  maxPoolSize: Threads,
  keepAliveTime: FiniteDuration,
  allowCoreTimeout: Boolean,
  prestartCore: Boolean
) {

  def toExecutor(name: String, queue: => BlockingQueue[Runnable]): Executor =
    Executor.fromThreadPoolExecutor(_ => yieldOpCount.n) {

      val threadPool = new ThreadPoolExecutor(
        corePoolSize.n,
        maxPoolSize.n,
        keepAliveTime.toMillis,
        MILLISECONDS,
        queue,
        threads.threadFactory(name, daemon = true)
      )

      threadPool.allowCoreThreadTimeOut(allowCoreTimeout)

      if (prestartCore) threadPool.prestartAllCoreThreads()

      threadPool
    }
}

object ExecutorConfig {

  implicit val configReader: ConfigReader[ExecutorConfig] = deriveReader
}

/** The Http configuration of the current application
  *
  * @param host The Host of the current application
  * @param port The Port to bind the current application to.
  */
final case class HttpCfg private[config](
  host: String,
  port: Int,
  connectorPoolSize: Option[Int],
  chunkFactor: Int,
  idleTimeout: Option[Duration],
  routes: RoutesConfig,
  maxConnections: Option[Int],
  responseTimeout: Option[Duration],
  executor: ExecutorsConfig
)

object HttpCfg {

  implicit val configReader: ConfigReader[HttpCfg] = deriveReader
}

/** The JWT configuration of the current application
  *
  * @param secret The secret
  * @param realm JWT provider which allows us to integrate with an existing authentication system
  */
final case class JwtCfg private[config](
  realm: String,
  secret: String
)

/** The Swagger configuration of the current application */
final case class SwaggerCfg private[config](
  contextPath: List[String],
  yamlName: String
)
object SwaggerCfg {

  implicit val configReader: ConfigReader[SwaggerCfg] = deriveReader
}
