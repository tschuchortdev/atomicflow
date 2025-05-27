ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.4"

val V = new {
  val cats = "2.13.0"
  val doobie = "1.0.0-RC9"
  val logbackClassic = "1.5.18"
  val munit = "1.1.1"
  val neotype = "0.3.25"
  val ox = "0.5.13"
  val upickle = "4.2.1"
}

lazy val root = (project in file("."))
  .settings(
    name := "atomicflow",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % V.logbackClassic % Test,
      "com.lihaoyi" %% "upickle" % V.upickle,
      "com.softwaremill.ox" %% "core" % V.ox,
      "de.lhns" %% "doobie-flyway" % "0.5.2",
      "io.github.kitlangton" %% "neotype" % V.neotype,
      "org.scalameta" %% "munit" % V.munit % Test,
      "org.tpolecat" %% "doobie-core" % V.doobie,
      "org.tpolecat" %% "doobie-postgres" % V.doobie,
      "org.tpolecat" %% "doobie-postgres-circe" % V.doobie,
      "org.tpolecat" %% "doobie-hikari" % V.doobie,
      "org.typelevel" %% "cats-core" % V.cats,
    )
  )
