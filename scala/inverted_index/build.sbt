import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "org.sparktw.codefight",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "sparktw-codefight",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
  )
parallelExecution in Test := false
