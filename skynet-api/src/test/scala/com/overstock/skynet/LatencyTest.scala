package com.overstock.skynet

import com.overstock.skynet.domain.Req.RankFrame
import com.overstock.skynet.http.Endpoints
import com.overstock.skynet.domain.{ExecStrategy, Frame, GetSample, RankingResult, Select}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client
import org.http4s.{Request, Response}
import plotly.element.BoxMean
import plotly.layout.{Axis, Layout}
import plotly.{Box, Plotly}
import sttp.tapir.client.http4s.Http4sClientInterpreter
import zio.clock.Clock
import zio.duration.Duration
import zio.{ExitCode, Has, RIO, Task, URIO, ZIO}
import zio.interop.catz._

object LatencyTest extends Endpoints with zio.App {

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = RIO.effectSuspend {

    val baseUri :: model :: Nil = args

    val nTrials   = 100
    val par       = 1
    val nRows     = 200 to 1000 by 200

    val contexts  = Nil
    val examples  = nRows.map(GetSample(model, _, contexts, None))
    val exec      = ExecStrategy.ParallelN(12)
    val idCol     = "product"
    val rankCol   = Select.field("prediction")
    val topK      = Some(3)
    val group     = Some(idCol)

    val createRankReq = (frame: Frame) => RankFrame(
      frame     = frame,
      modelName = model,
      idCol     = idCol,
      group     = group,
      rankCol   = rankCol,
      topK      = topK,
      exec      = Some(exec)
    )

    ZIO
      .runtime[zio.ZEnv]
      .flatMap { implicit runtime =>

        val interpreter = Http4sClientInterpreter[Task]()

        val getSample = interpreter.toRequestUnsafe(getSampleInput, Some(baseUri))

        val rank = interpreter.toRequestUnsafe(rankEndpoint, Some(baseUri))

        val client = BlazeClientBuilder[Task](runtime.platform.executor.asEC)

        for {

          times <- client.resource.use(
                    client =>
                      ZIO.foreach(examples) {
                        sample =>
                          runRanks(
                            client,
                            getSample,
                            createRankReq andThen rank,
                            nTrials = nTrials,
                            par = par)(sample)
                            .map(
                              times =>
                                Box()
                                  .withX(times.map(_.toMillis).filter(_ < 1000))
                                  .withBoxmean(BoxMean.True)
                                  .withName(sample.nRows.toString))
                      }.provideLayer(Clock.live))


        } yield {

          val path = os.pwd / "plots" / "rank" / "latency" / model / exec.toString

          val file = path / "plot.html"

          os.makeDir.all(path)

          file.toIO.delete()

          Plotly.plot(
            path = file.toString,
            addSuffixIfExists = false,
            traces = times,
            layout = Layout()
              .withTitle(s"Rank Latency | Model: $model | Exec: $exec")
              .withYaxis(Axis().withTitle("Nunber of Rows per Request"))
              .withXaxis(Axis().withTitle("Request Latency (millis)").withDomain(0.0, 1000.0))
          )
        }
      }
  }.exitCode

  type Req[F[_], A] = (Request[F], Response[F] => F[Either[Throwable, A]])

  def runRanks(
    client: Client[Task],
    getSampleF: GetSample => Req[Task, Frame],
    rank: Frame => Req[Task, RankingResult],
    nTrials: Int = 50,
    par: Int = 4
  ): GetSample => RIO[Clock, List[Duration]] = getSample => {
    val (sampleReq, decode) = getSampleF(getSample)
    for {
      frame <- client.run(sampleReq).use(decode).absolve
      (sampleRank, decodeRank) = rank(frame)
      times <- ZIO.collectAllParN(par)(
                List.fill(nTrials)(
                  client
                    .run(sampleRank)
                    .use(decodeRank)
                    .timed.map(_._1)))
    } yield times
  }
}
