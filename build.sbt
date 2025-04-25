ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.4"

val V = new {
  val logbackClassic = "1.5.18"
  val munit = "1.1.0"
  val iron = "3.0.0"
  val cats = "2.13.0"
}

lazy val root = (project in file("."))
  .settings(
    name := "atomicflow",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % V.logbackClassic % Test,
      "org.scalameta" %% "munit" % V.munit % Test,
      "io.github.iltotore" %% "iron" % V.iron,
      "org.typelevel" %% "cats-core" % V.cats
    )
  )
