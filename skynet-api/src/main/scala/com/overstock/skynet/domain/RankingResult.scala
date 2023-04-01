package com.overstock.skynet.domain

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import io.circe.syntax.EncoderOps
import sttp.tapir.Schema
import sttp.tapir.Schema.annotations.{description, encodedExample}

@description("A ranked list of result ids")
@encodedExample(RankingResult(Seq("1", "2", "3")).asJson.spaces4)
case class RankingResult(results: Seq[String])

object RankingResult {

  implicit val rankingResultCodec: Codec[RankingResult] = deriveCodec

  implicit val schemaRankingResult: Schema[RankingResult] = Schema.derived
}
