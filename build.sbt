lazy val commonSettings = commonSmlBuildSettings ++ ossPublishSettings ++ Seq(
  organization := "com.softwaremill",
  scalaVersion := "2.12.6"
)

val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % "test"

lazy val rootProject = (project in file("."))
  .settings(commonSettings: _*)
  .settings(publishArtifact := false, name := "akka-vs-scalaz")
  .aggregate(core)

lazy val akkaVersion = "2.5.12"

lazy val core: Project = (project in file("core"))
  .settings(commonSettings: _*)
  .settings(
    name := "core",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit-typed" % akkaVersion % Test,
      "org.scalaz" %% "scalaz-ioeffect" % "2.2.0",
      "io.monix" %% "monix" % "3.0.0-RC1",
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
      scalaTest
    )
  )
