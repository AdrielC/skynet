package com.overstock

import com.overstock.skynet.http.Routes
import com.overstock.skynet.service.config.Config
import com.overstock.skynet.service.model.ModelEnv
import com.overstock.skynet.util.threads._
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
