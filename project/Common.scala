package com.overstock.skynet

import sbt._
import Keys._
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.AssemblyPlugin
import scala.language.postfixOps

object Common {

  name := "skynet"

  /* Override `Artifact.artifactName`. We do not want to have the Scala version
   * the file name. (https://www.scala-sbt.org/1.x/docs/Artifacts.html) */
  artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
    artifact.name + "-" + module.revision + "." + artifact.extension
  }

  lazy val commonSettings = Seq(
    organization := "com.overstock",
    scalaVersion := "2.12.9",
    version := "0.3.3",
    resolvers ++= Seq(
      DefaultMavenRepository,
      Resolver.jcenterRepo,
      Resolver.sbtPluginRepo("releases"),
      Resolver.mavenLocal,
      Resolver.sonatypeRepo("releases"),
      Resolver.sonatypeRepo("snapshots")
    ),
    scalacOptions ++= CompilerOptions.cOptions,
    test in assembly := {},
    addCompilerPlugin("org.typelevel" % "kind-projector" % "0.11.3" cross CrossVersion.full)
  )

  def baseProject(name: String): Project =
    Project(name, file(name))
      .settings(commonSettings: _*)
      .configs(IntegrationTest)
      .settings(Defaults.itSettings)
}
