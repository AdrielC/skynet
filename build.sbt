import com.overstock.skynet._
import sbt._
import sbt.Keys._
import Common._
import sbtdocker.DockerPlugin.autoImport.ImageName

import scala.language.postfixOps

lazy val basePackage = "com.overstock.skynet"
lazy val mainClassName = s"$basePackage.Starter"

 lazy val `skynet` = (project in file("."))
  .aggregate(`skynet-api`)

lazy val `skynet-api` = baseProject("skynet-api")
  .enablePlugins(BuildInfoPlugin)
  .enablePlugins(DockerPlugin)
  .enablePlugins(AssemblyPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := basePackage,
    libraryDependencies ++= Dependencies.skynetapi,
    libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.8.1",
    mainClass in assembly := Some(mainClassName),
    assemblyMergeStrategy in assembly := {
      case PathList("module-info.class") => MergeStrategy.discard
      case s if s.contains("UnusedStubClass.class") => MergeStrategy.discard
      case PathList("META-INF", "maven", "org.webjars", "swagger-ui", "pom.properties") => MergeStrategy.concat
      case PathList("google", "protobuf", _@ _*) => MergeStrategy.discard
      case x => MergeStrategy.defaultMergeStrategy(x)
    },
    dockerSettings
  )

lazy val dockerSettings = Seq(
  docker / imageNames := Seq(
    // Sets the latest tag
    ImageName(s"${organization.value}/${name.value}:latest"),

    // Sets a name with a tag that contains the project version
    ImageName(
      namespace = Some(organization.value),
      repository = name.value,
      tag = Some("v" + version.value)
    )
  ),
  docker / dockerfile := {
    val artifact: File = assembly.value
    val jarTarget = s"/app/${artifact.name}"

    new Dockerfile {
      from("openjdk:11-jre-slim-buster")
      workDir("/opt/docker")
      add(artifact, jarTarget)
      run("apt-get", "update")
      run("apt-get", "install", "libgomp1") // required for xgboost
      run("apt-get", "-y", "install", "graphviz") // required for reftree

      expose(80)
      entryPoint(
        "java",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+UseContainerSupport",
        "-XX:MaxRAMPercentage=95.0",
        "-classpath", s"$jarTarget",
        (mainClass in assembly).value.get
      )
    }
  },
  docker / buildOptions := BuildOptions(
    cache = false,
    removeIntermediateContainers = BuildOptions.Remove.Always,
    pullBaseImage = BuildOptions.Pull.Always,
    additionalArguments = Seq("--shm-size=512mb")
  )
)

lazy val runServer = inputKey[Unit]("Runs web-server")
lazy val runStart = inputKey[Unit]("Runs web-server")

runServer := (run in `skynet-api` in Compile).evaluated