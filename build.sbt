ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.4"

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
