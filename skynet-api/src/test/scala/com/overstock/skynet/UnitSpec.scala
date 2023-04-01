package com.overstock.skynet

import org.scalamock.scalatest.AsyncMockFactory
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.time.{Minute, Seconds, Span}

abstract class UnitSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with AsyncMockFactory
    with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(1, Minute)),
    interval = scaled(Span(15, Seconds))
  )
}
