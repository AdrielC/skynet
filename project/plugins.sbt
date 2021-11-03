addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.0.0")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.6.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.15.0")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.8.2")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.11")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.4.2")
addSbtPlugin("ch.epfl.scala" % "sbt-bloop" % "1.3.2")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.9.0")
addSbtPlugin("io.spray" % "sbt-revolver" % "0.9.1")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.10.0-RC1")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.1"

scalacOptions += "-Ypartial-unification"
