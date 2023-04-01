package com.overstock.skynet.domain

import com.overstock.skynet.util.render.OpExpr.ModelGraphOptions
import reftree.render.RenderingOptions

case class GraphModel(modelName: String,
                      elideSchemaForParent: Boolean,
                      uniqueSchemas: Boolean,
                      density: Int,
                      verticalSpacing: Double) {

  implicit val renderingOptions: RenderingOptions = RenderingOptions(
    density = density,
    verticalSpacing = verticalSpacing)

  implicit val graphOptions: ModelGraphOptions = ModelGraphOptions(
    elideSchemaForParent = elideSchemaForParent,
    uniqueSchemas = uniqueSchemas)

}
