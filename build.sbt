ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.4"

val V = new {
  val cats = "2.13.0"
  val doobie = "1.0.0-RC9"
  val iron = "3.0.1"
  val logbackClassic = "1.5.18"
  val munit = "1.1.1"
  val ox = "0.5.13"
}

lazy val root = (project in file("."))
  .settings(
    name := "atomicflow",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % V.logbackClassic % Test,
      "com.softwaremill.ox" %% "core" % V.ox,
      "io.github.iltotore" %% "iron" % V.iron,
      "org.scalameta" %% "munit" % V.munit % Test,
      "org.tpolecat" %% "doobie-core" % V.doobie,
      "org.typelevel" %% "cats-core" % V.cats,
    )
  )
