package exd.fundamenski.skynet

import exd.fundamenski.skynet.service.config.{Config, loadConfig}
import exd.fundamenski.skynet.service.threads.Async
import exd.fundamenski.skynet.service.model.{Models, ModelsCache, ModelsService}
import zio.{ExitCode, Has, UIO, URIO, ZLayer}
import exd.fundamenski.skynet.service.repository.Repository
import exd.fundamenski.skynet.http.Routes
import exd.fundamenski.skynet.service.config.SkynetConfig
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
|___________                .___           __________      .__        __   
|\_   _____/__  _______   __| _/_ __  _____\______   \____ |__| _____/  |_ 
| |    __)_\  \/  /  _ \ / __ |  |  \/  ___/|     ___/  _ \|  |/    \   __\
| |        \>    <  <_> ) /_/ |  |  /\___ \ |    |  (  <_> )  |   |  \  |  
|/_______  /__/\_ \____/\____ |____//____  >|____|   \____/|__|___|  /__|  
|        \/      \/          \/          \/                        \/      
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

  val program = UIO {
    logger.info(banner)
    logger.info(BuildInfo.toString)
    logger.info("Available processors: " + Runtime.getRuntime.availableProcessors())
    logger.info("Default model execution strategy: " + conf.config.model.executionStrategy)
  } *> Server.run

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    program
    .provideCustomLayer(serviceLayers)
    .exitCode
}
