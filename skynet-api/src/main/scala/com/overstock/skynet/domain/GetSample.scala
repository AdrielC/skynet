package com.overstock.skynet.domain

case class GetSample(modelName: String,
                     nRows: Int,
                     contextPrefixes: List[String],
                     select: Option[Select])
