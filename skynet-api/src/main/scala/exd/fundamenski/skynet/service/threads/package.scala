package exd.fundamenski.skynet.service

import exd.fundamenski.skynet.service.config.ExecutorsConfig
import zio.blocking.Blocking
import zio.internal.Executor
import zio.{Has, ZLayer}

package object threads {

  type Async = Has[Async.Service]
  object Async {
    trait Service {
      def asyncExecutor: Executor
    }

    val fromConfig: ZLayer[Has[ExecutorsConfig], Nothing, Async] =
      ZLayer.fromService((config: ExecutorsConfig) => new Async.Service {
        lazy val asyncExecutor: Executor = config.createAsync
      })
  }


  object Blocking {

    val fromConfig: ZLayer[Has[ExecutorsConfig], Nothing, Blocking] =
      ZLayer.fromService((config: ExecutorsConfig) => new zio.blocking.Blocking.Service {
        lazy val blockingExecutor: Executor = config.createBlocking
      })
  }
}
