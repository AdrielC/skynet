package com.overstock.skynet

import com.typesafe.scalalogging.LazyLogging
import org.log4s.getLogger
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

abstract class IntegrationTest
    extends AsyncWordSpec
    with Matchers
    with BeforeAndAfter
    with BeforeAndAfterAll
    with ScalaFutures {

  private[this] val logger = getLogger

  implicit val patience = PatienceConfig(timeout = Span(5, Seconds), interval = Span(500, Millis))

  override def beforeAll(): Unit = {
    logger.info("Starting application")
  }

  override def afterAll(): Unit = {
    logger.info("Stopping application...")
  }

}