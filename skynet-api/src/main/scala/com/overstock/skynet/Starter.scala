package com.overstock.skynet

import com.overstock.skynet.http.Routes
import com.overstock.skynet.service.config.{Config, SkynetConfig, loadConfig}
import com.overstock.skynet.service.threads.Async
import com.overstock.skynet.service.model.{Models, ModelsCache, ModelsService}
import zio.{ExitCode, Has, UIO, URIO, ZLayer}
import com.overstock.skynet.service.repository.Repository
import org.log4s.{Logger, getLogger}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.Console
import zio.internal.{Executor, Platform}
import zio.random.Random
import zio.system.System

object Starter extends zio.App {

  private[this] implicit val logger: Logger = getLogger

  private val banner = """
  |
  |eeeee e   e  e    e    eeeee eeee eeeee
  |8   " 8   8  8    8    8   8 8      8
  |8eeee 8eee8e 8eeee8    8e  8 8eee   8e
  |   88 88   8   88      88  8 88     88
  |8ee88 88   8   88      88  8 88ee   88
  |
  |""".stripMargin

  private lazy val conf: Config.Service = new Config.Service {
    override val config: SkynetConfig = loadConfig[SkynetConfig].toTry.get
  }

  private lazy val async: Async.Service = new Async.Service {
    override val asyncExecutor: Executor = conf.config.http.executor.createAsync
  }


  private lazy val blocking: Blocking.Service = new Blocking.Service {
    override val blockingExecutor: Executor = conf.config.model.executor.createBlocking
  }

  override lazy val environment: zio.ZEnv = Has.allOf[
    Clock.Service, Console.Service, System.Service, Random.Service, Blocking.Service](
    Clock.Service.live,
    Console.Service.live,
    System.Service.live,
    Random.Service.live,
    blocking
  )

  lazy val serviceLayers =
    (ZLayer.succeed(conf) >+>
      (ZLayer.succeed(blocking) ++
        ZLayer.succeed(async) ++
        Repository.live ++
        Clock.live ++
        Console.live ++
        Models.live ++
        Routes.live)) >+>
      ModelsCache.live >+>
      ModelsService.live

  override lazy val platform: Platform = Platform.fromExecutor(async.asyncExecutor)

  private val program = UIO {
    logger.info(banner)
    logger.info(BuildInfo.toString)
    logger.info("Available processors: " + Runtime.getRuntime.availableProcessors())
    logger.info("Default model execution strategy: " + conf.config.model.executionStrategy)
  } *> Server.run

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program.provideLayer(serviceLayers).exitCode
}
