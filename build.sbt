import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.3"

val dependencyVersions = new {
  val circe = "0.14.14"
  val doobie = "1.0.0-RC9"
  val flywayPostgres = "11.10.0"
  val logbackClassic = "1.5.18"
  val munit = "1.1.1"
  val ox = "0.7.0"
  val slf4j = "2.0.17"
  val upickle = "4.2.1"
  val testcontainers = "1.21.3"
}

lazy val root = (project in file("."))
  .settings(
    name := "atomicflow",
    publishArtifact := false,
    publish / skip := true
  )
  .aggregate(core, db, example)

lazy val core = project
  .settings(
    name := "atomicflow-core",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % dependencyVersions.logbackClassic % Test,
      "com.lihaoyi" %% "upickle" % dependencyVersions.upickle,
      "com.softwaremill.ox" %% "core" % dependencyVersions.ox,
      "org.scalameta" %% "munit" % dependencyVersions.munit % Test,
      "org.slf4j" % "slf4j-api" % dependencyVersions.slf4j,
    )
  )

lazy val db = project
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "atomicflow-db",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % dependencyVersions.logbackClassic % Test,
      "org.flywaydb" % "flyway-database-postgresql" % dependencyVersions.flywayPostgres,
      "org.tpolecat" %% "doobie-core" % dependencyVersions.doobie,
      "org.tpolecat" %% "doobie-postgres" % dependencyVersions.doobie,
      "org.tpolecat" %% "doobie-hikari" % dependencyVersions.doobie,
      "org.testcontainers" % "testcontainers" % dependencyVersions.testcontainers % Test,
      "org.testcontainers" % "postgresql" % dependencyVersions.testcontainers % Test
    )
  )

lazy val example = project
  .dependsOn(core, db)
  .settings(
    name := "atomicflow-examples",
    libraryDependencies ++= Seq(
      "org.business4s" %% "workflows4s-core" % "0.4.0",
      "org.business4s" %% "workflows4s-bpmn" % "0.4.0",
      "org.typelevel" %% "cats-effect" % "3.6.1",
      "org.typelevel" %% "cats-core" % "2.13.0",
      "org.typelevel" %% "cats-mtl" % "1.5.0",
      "org.typelevel" %% "kittens" % "3.5.0",
      "org.http4s" %% "http4s-jdk-http-client" % "0.10.0",
      "org.http4s" %% "http4s-dsl" % "0.23.30",
      "co.fs2" %% "fs2-core" % "3.12.0",
      "co.fs2" %% "fs2-io" % "3.12.0",
      "co.fs2" %% "fs2-reactive-streams" % "3.12.0",
      "io.circe" %% "circe-core" % dependencyVersions.circe,
      "io.circe" %% "circe-parser" % dependencyVersions.circe,
      "io.circe" %% "circe-generic" % dependencyVersions.circe,
    ),
    // Test dependencies
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % dependencyVersions.munit,
      "org.testcontainers" % "testcontainers" % "1.21.3",
      "org.testcontainers" % "postgresql" % "1.21.3",
      "org.scalamock" %% "scalamock" % "7.5.0",
      "org.scalamock" %% "scalamock-cats-effect" % "7.5.0"
    ).map(_ % Test)
  )
