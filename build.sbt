import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.7.3"

val V = new {
  val cats = "2.13.0"
  val circe = "0.14.14"
  val doobie = "1.0.0-RC9"
  val flywayPostgres = "11.10.0"
  val logbackClassic = "1.5.18"
  val munit = "1.1.1"
  val neotype = "0.3.25"
  val ox = "0.7.0"
  val upickle = "4.2.1"
}

lazy val root = (project in file("."))
  //.settings(commonSettings)
  .settings(
    name := "atomicflow",
    publishArtifact := false,
    publish / skip := true
  )
  .aggregate(core, db)

lazy val core = project
  .settings(
    name := "atomicflow-core",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % V.logbackClassic % Test,
      "com.lihaoyi" %% "upickle" % V.upickle,
      "com.softwaremill.ox" %% "core" % V.ox,
      "io.circe" %% "circe-generic" % V.circe,
      "io.github.kitlangton" %% "neotype" % V.neotype,
      "org.scalameta" %% "munit" % V.munit % Test,
      "org.typelevel" %% "cats-core" % V.cats,
    )
  )

lazy val db = project
  .dependsOn(core % "compile->compile;test->test")
  .settings(
    name := "atomicflow-db",
    libraryDependencies ++= Seq(
      "de.lhns" %% "doobie-flyway" % "0.5.2",
      "org.flywaydb" % "flyway-database-postgresql" % V.flywayPostgres,
      "org.tpolecat" %% "doobie-core" % V.doobie,
      "org.tpolecat" %% "doobie-postgres" % V.doobie,
      "org.tpolecat" %% "doobie-postgres-circe" % V.doobie,
      "org.tpolecat" %% "doobie-hikari" % V.doobie,
    )
  )

lazy val example = project
  .dependsOn(core, db)
  .settings(
    name := "atomicflow-examples",
    resolvers += ("gitlab-basis-consumer-lib" at "http://gitlab.zpc.bms.ads/api/v4/projects/318/packages/maven")
      .withAllowInsecureProtocol(true),
    libraryDependencies ++= Seq(
      "org.business4s" %% "workflows4s-core" % "0.4.0",
      "org.typelevel" %% "cats-effect" % "3.6.1",
      "org.typelevel" %% "cats-core" % "2.13.0",
      "org.typelevel" %% "cats-mtl" % "1.5.0",
      "org.typelevel" %% "kittens" % "3.5.0",
      "org.http4s" %% "http4s-jdk-http-client" % "0.10.0",
      "org.http4s" %% "http4s-dsl" % "0.23.30",
      "co.fs2" %% "fs2-core" % "3.12.0",
      "co.fs2" %% "fs2-io" % "3.12.0",
      "co.fs2" %% "fs2-reactive-streams" % "3.12.0",
      "de.bitmarck" %% "basis-consumer-lib" % "0.0.7",
      "de.lhns" %% "scala-trustmanager-utils" % "1.1.0",

    ),
    // Test dependencies
    libraryDependencies ++= Seq(
      "org.testcontainers" % "testcontainers" % "1.21.3",
      "org.testcontainers" % "postgresql" % "1.21.3"
    ).map(_ % Test)
  )
