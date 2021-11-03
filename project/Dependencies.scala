import sbt._
import sbt.librarymanagement.InclExclRule

object Dependencies {

    object Version {
      val akka            = "2.6.14"
      val akkaCors        = "0.4.0"
      val akkaHttp        = "10.1.8"
      val cats            = "1.6.0"
      val circe           = "0.14.1"
      val typesafeConfig  = "1.3.4"
      val pureConfig      = "0.11.0"
      val scalatest       = "3.0.7"
      val scalaMock       = "4.2.0"
      val mockito         = "1.10.19"
      val scalaLogging    = "3.9.2"
      val logback         = "1.2.3"
      val jwtAuthentikat  = "0.4.5"
      val janinoVersion   = "3.0.12"
      val swaggerUi       = "3.22.2"
      val tapir           = "0.19.0-M2"
      val mleap           = "0.16.0"
      val xgbPredictor    = "0.3.17"
      val reftree         = "1.4.1"
      val awsSdk          = "1.11.349"
      val mlflow          = "1.17.0"
      val gs              = "1.113.16"
      val os              = "0.7.3"
      val scopt           = "3.5.0"
      val droste          = "0.8.0"
      val prometheus      = "0.1.0"
      val http4s          = "0.23.1"
      val agate           = "0.0.10"
    }

  object Include {

    // Http4s
    val http4s            = "org.http4s" %% "http4s-client" % Version.http4s
    val http4sPrometheus  = "org.http4s" %% "http4s-prometheus-metrics" % Version.http4s
    val http4sCirce       = "org.http4s" %% "http4s-circe" % Version.http4s
    val http4sBlaze       = "org.http4s" %% "http4s-blaze-client" % Version.http4s

    // ZIO
    val zioCache          = "dev.zio" %% "zio-cache" % "0.1.0"

    // Tapir
    val http4sClient      = "com.softwaremill.sttp.tapir" %% "tapir-http4s-client" % Version.tapir
    val tapirhttp4sZIO    = "com.softwaremill.sttp.tapir" %% "tapir-zio-http4s-server" % Version.tapir
    val tapirZIOHttp      = "com.softwaremill.sttp.tapir" %% "tapir-zio-http" % Version.tapir

    // Mleap
    lazy val mleapRuntime = "ml.combust.mleap" %% "mleap-runtime" % Version.mleap
    lazy val mleapExecutor = ("ml.combust.mleap" %% "mleap-executor" % Version.mleap)
      .excludeAll(InclExclRule().withOrganization("com.typesafe.akka"))

    // Models
    lazy val xgbPredictor = "ai.h2o" % "xgboost-predictor" % Version.xgbPredictor exclude("com.esotericsoftware.kryo", "kryo")
    val agate = "com.stripe" %% "agate-core" % Version.agate

    // JSON/Protobuf
    lazy val circeCore = "io.circe" %% "circe-core" % Version.circe
    lazy val circeGeneric = "io.circe" %% "circe-generic" % Version.circe
    lazy val circeGenericExtrax = "io.circe" %% "circe-generic-extras" % Version.circe
    lazy val circeParser = "io.circe" %% "circe-parser" % Version.circe

    lazy val scalapbCirce = "io.github.scalapb-json" %% "scalapb-circe" % "0.2.1"
    lazy val scalapbJson4s = "com.thesamet.scalapb" %% "scalapb-json4s" % scalapb.compiler.Version.scalapbVersion


    // Configuration
    lazy val typesafeConfig = "com.typesafe" % "config" % Version.typesafeConfig
    lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % Version.pureConfig

    // Typeclasses
    lazy val catsCore = "org.typelevel" %% "cats-core" % Version.cats
    lazy val droste = "io.higherkindness" %% "droste-core" % Version.droste

    // Documentation
    lazy val swaggerUiHtt4s = "com.softwaremill.sttp.tapir" %% "tapir-swagger-ui-http4s" % Version.tapir
    lazy val tapirCore = "com.softwaremill.sttp.tapir" %% "tapir-core" % Version.tapir
    lazy val tapirOpenApi = "com.softwaremill.sttp.tapir" %% "tapir-openapi-docs" % Version.tapir
    lazy val tapirOpenApiCirceYaml = "com.softwaremill.sttp.tapir" %% "tapir-openapi-circe-yaml" % Version.tapir
    lazy val reftree = "com.kyleu" %% "reftree" % Version.reftree

    // Metrics
    val prometheusClient = "io.prometheus" % "simpleclient"        % Version.prometheus
    val prometheusCommon = "io.prometheus" % "simpleclient_common" % Version.prometheus


    // Repository/Filesystem
    val os      = "com.lihaoyi" %% "os-lib" % Version.os
    val awsS3   = "com.amazonaws" % "aws-java-sdk-s3" % Version.awsSdk
    val mlflow  = "org.mlflow" % "mlflow-client" % Version.mlflow
    val gs      = ("com.google.cloud" % "google-cloud-storage" % Version.gs)
      .excludeAll(InclExclRule().withOrganization("com.google.protobuf"))

    // Logging
    lazy val logbackClassic = "ch.qos.logback" % "logback-classic" % Version.logback
    lazy val scalaLogging   = "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
    lazy val janino         = "org.codehaus.janino" % "janino" % Version.janinoVersion

    // Testing
    lazy val scalatest = "org.scalatest" %% "scalatest" % Version.scalatest % "it,test"
    lazy val scalaMock = "org.scalamock" %% "scalamock" % Version.scalaMock % "it,test"
    lazy val mockito = "org.mockito" % "mockito-all" % Version.mockito % "it,test"
  }

  lazy val ml: Seq[ModuleID] = Seq(
    Include.mleapExecutor,
    Include.xgbPredictor
  )

  lazy val config: Seq[ModuleID] = Seq(
    Include.pureConfig
  )

  lazy val testing: Seq[ModuleID] = Seq(
    Include.mockito,
    Include.scalatest,
    Include.scalaMock
  )

  lazy val logging: Seq[ModuleID] = Seq(
    Include.logbackClassic,
    Include.janino
  )

  lazy val json: Seq[ModuleID] = Seq(
    Include.circeParser,
    Include.circeGeneric,
    Include.scalapbCirce,
    Include.http4sCirce
  )

  lazy val http: Seq[ModuleID] = Seq(
    Include.tapirhttp4sZIO,
    Include.tapirCore,
    Include.tapirZIOHttp,
    Include.zioCache,
    Include.http4sClient,
    Include.http4s,
    Include.http4sBlaze
  )

  lazy val repo: Seq[ModuleID] = Seq(
    Include.awsS3,
    Include.mlflow,
    Include.gs,
    Include.os
  )

  lazy val metrics = Seq(
    Include.prometheusClient,
    Include.prometheusCommon,
    Include.http4sPrometheus
  )

  lazy val docs = Seq(
    Include.swaggerUiHtt4s,
    Include.tapirOpenApi,
    Include.tapirOpenApiCirceYaml,
    Include.reftree
  )

  lazy val util = Seq(
    Include.droste
  )

  val skynetapi: Seq[ModuleID] =
      ml      ++
      json    ++
      logging ++
      config  ++
      http   ++
      repo    ++
      metrics ++
      docs    ++
      util    ++
      testing
}
