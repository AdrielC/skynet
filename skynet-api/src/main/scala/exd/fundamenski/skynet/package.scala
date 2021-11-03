package exd.fundamenski

import exd.fundamenski.skynet.http.Routes
import exd.fundamenski.skynet.service.config.Config
import exd.fundamenski.skynet.service.model.ModelEnv
import exd.fundamenski.skynet.util.threads._
import zio.{Has, ZEnv}
import zio.internal.Executor

package object skynet {

  type AppEnv = ModelEnv with Has[Routes] with Config with ZEnv

  private[skynet] object default {

    val blockingExecutor: Executor =
      Executor.fromThreadPoolExecutor(_ => Int.MaxValue) {
        newBlockingPool("skynet-default-blocking")
      }

    val asyncExecutor: Executor =
      Executor.fromThreadPoolExecutor(_ => default.yieldOpCount) {
        newDaemonPool("skynet-default-async")
      }

    val yieldOpCount = 2048
  }
}
