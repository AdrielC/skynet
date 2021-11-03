package exd.fundamenski.skynet.domain

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import ml.combust.mleap.json.circe._
import exd.fundamenski.skynet.util.json._
import sttp.tapir.Schema

import java.net.URI

case class Model(modelName: String, uri: URI)

object Model {

  implicit val modelCodec: Codec[Model] = deriveCodec
  implicit val modelSchema: Schema[Model] = Schema.derived[Model]
}