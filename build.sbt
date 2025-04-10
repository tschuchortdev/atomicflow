ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.6.4"

lazy val root = (project in file("."))
  .settings(
    name := "atomicflow",
    libraryDependencies ++= Seq(
      "io.github.iltotore" %% "iron" % "3.0.0"
    )
  )
