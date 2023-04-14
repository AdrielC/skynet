package com.overstock.skynet

import com.overstock.skynet.domain.Req.RankFrame
import com.overstock.skynet.domain.{GetSample, Select}
import com.overstock.skynet.http.Routes
import com.overstock.skynet.service.model.ModelTask
import zio.{Has, ZEnv, ZIO}
import sttp.tapir.client.http4s.Http4sClientInterpreter
import zio.interop.catz._
import org.http4s.client._
import org.http4s.implicits.http4sLiteralsSyntax
import sttp.tapir.DecodeResult

class StarterIt extends IntegrationTest {

  "Service" should {

    "Rank" in {

      zio.Runtime.default.unsafeRunToFuture((for {
        _ <- Starter.run(Nil).forkDaemon
        (clientInterpreter, routes, client)  <- ZIO.runtime[Has[Routes]]
          .map { implicit rt =>
            val clientInterpreter = Http4sClientInterpreter[ModelTask]()
            val routes = rt.environment.get[Routes]
            val client = Client.fromHttpApp(routes.app)
            (clientInterpreter, routes, client)
          }

        endpoints           = routes.endPoints
        getSample           = clientInterpreter.toRequest(endpoints.getSampleInput, baseUri = Some(uri"localhost"))
        rank                = clientInterpreter.toRequest(endpoints.rankEndpoint, baseUri = Some(uri"localhost"))

        (sampleReq, decode) = getSample(GetSample("evrln-ltr", 200, List("context_sku_ids|user", "context"), None))

        c <- client.run(sampleReq).use(decode).flatMap {
          case failure: DecodeResult.Failure => ZIO.fail(new Error(failure.toString))
          case DecodeResult.Value(Left(v)) => ZIO.fail(v)
          case DecodeResult.Value(Right(v)) => ZIO.succeed(v)
        }

        (sampleRank, decodeRank) = rank(RankFrame(
          frame = c,
          modelName = "evrln-ltr",
          idCol = "resul_sku_id",
          rankCol = Select.field("prediction")
        ))

        res <- client.run(sampleRank).use(decodeRank).flatMap {
          case failure: DecodeResult.Failure => ZIO.fail(new Error(failure.toString))
          case DecodeResult.Value(Left(v)) => ZIO.fail(v)
          case DecodeResult.Value(Right(v)) => ZIO.succeed(v)
        }

      } yield res.results should not have length (0))
        .provideLayer(ZEnv.live ++ Starter.serviceLayers))
    }
  }
}
