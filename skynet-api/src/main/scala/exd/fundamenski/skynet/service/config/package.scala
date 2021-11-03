package exd.fundamenski.skynet.service

import cats.data.NonEmptyList
import pureconfig.{ConfigReader, Derivation}
import pureconfig.backend.ConfigFactoryWrapper.{invalidateCaches, load}
import pureconfig.error.{ConfigReaderException, ConfigReaderFailure, ConfigReaderFailures}
import shapeless.ops.product.ToTuple
import shapeless.Lazy
import zio.{Has, IO, Layer, ZIO, ZLayer}

import scala.reflect.ClassTag

package object config {

  type Config = Has[Config.Service]
  object Config {
    trait Service {
      def config: SkynetConfig
    }

    val live: Layer[ConfigReaderException[SkynetConfig], Config] =
      ZLayer.fromEffect(loadConfigTask[SkynetConfig]
        .map(conf => new Config.Service {
          val config: SkynetConfig = conf
        }))
  }



  implicit class NelErrorOps[A](private val fails: A) extends AnyVal {

    def toNel(implicit G: Lazy[ToTuple.Aux[A, (ConfigReaderFailure, List[ConfigReaderFailure])]])
    : NonEmptyList[ConfigReaderFailure] = (NonEmptyList[ConfigReaderFailure](_, _)).tupled(G.value(fails))

    def toFailures(implicit G: Lazy[ToTuple.Aux[A, (ConfigReaderFailure, List[ConfigReaderFailure])]])
    : ConfigReaderFailures = (ConfigReaderFailures(_: ConfigReaderFailure, _: List[ConfigReaderFailure]))
      .tupled(G.value(fails))
  }

  def range(floor: Int, desired: Int, ceiling: Int): Int =
    scala.math.min(scala.math.max(floor, desired), ceiling)

  def loadConfig[Config: ClassTag]
  (implicit reader: Derivation[ConfigReader[Config]]): Either[ConfigReaderException[Config], Config] =
    (for {
      _         <- invalidateCaches().right
      rawConfig <- load().right
      config    <- pureconfig.loadConfig(rawConfig).right
    } yield config).left.map(ConfigReaderException[Config](_))

  def loadConfigTask[Config: ClassTag]
  (implicit reader: Derivation[ConfigReader[Config]]): IO[ConfigReaderException[Config], Config] =
    ZIO.fromEither(loadConfig[Config])
}
