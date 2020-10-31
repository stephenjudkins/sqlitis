scalaVersion := "2.13.3"

Global / onChangedBuildSource := ReloadOnSourceChanges

Global / testFrameworks += new TestFramework("utest.runner.Framework")

val ourOptions = scalacOptions ~= (_.filterNot(o => Set(
  "-Ywarn-unused:imports",
  "-Ywarn-unused:params",
  "-Ywarn-unused:implicits"
)(o)))

lazy val core = (project in file("core"))
  .settings(
    libraryDependencies ++= Seq(
      "org.tpolecat" %% "atto-core" % "0.8.0",
      "com.chuusai" %% "shapeless" % "2.3.3",
      "com.lihaoyi" %% "utest" % "0.7.5" % Test
    ),
    ourOptions
  )

lazy val doobie = (project in file("doobie"))
  .dependsOn(core)
  .settings(
    libraryDependencies ++= Seq(
      "org.tpolecat" %% "doobie-core" % "0.9.0",
      "org.tpolecat" %% "doobie-h2" % "0.9.0" % Test,
      "com.lihaoyi" %% "utest" % "0.7.5" % Test
    ),
    ourOptions
  )