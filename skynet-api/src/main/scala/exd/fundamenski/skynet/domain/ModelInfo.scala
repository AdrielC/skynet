package exd.fundamenski.skynet.domain

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import ml.combust.bundle.dsl.BundleInfo
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.executor.BundleMeta
import ml.combust.mleap.json.circe._
import ml.combust.mleap.json.circe._
import exd.fundamenski.skynet.util.json._
import io.circe.syntax.EncoderOps
import sttp.tapir.Schema

import java.net.URI

case class ModelInfo(model: Model, bundleMeta: BundleMeta)

object ModelInfo {

  def apply(modelName: String,
            uri: URI,
            info: BundleInfo,
            inputSchema: StructType,
            outputSchema: StructType): ModelInfo =
    ModelInfo(
      model = Model(
        modelName     = modelName,
        uri           = uri),
      bundleMeta  = BundleMeta(
        info          = info,
        inputSchema   = inputSchema,
        outputSchema  = outputSchema))

  import exd.fundamenski.skynet.util.json._

  implicit val attrSchema: Schema[ml.bundle.Attributes] = {
    import exd.fundamenski.skynet.util.json.ProtobufConverters._
    schemaForCirceJson.map(_.as[ml.bundle.Attributes].toOption)(_.asJson)
  }

  implicit val modelInfoSchema: Schema[ModelInfo] =
    sttp.tapir.generic.auto.schemaForCaseClass[ModelInfo].value

  implicit val modelInfoCodec: Codec[ModelInfo] = deriveCodec
}