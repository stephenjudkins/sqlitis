scalaVersion := "2.13.3"

Global / onChangedBuildSource := ReloadOnSourceChanges

Global / testFrameworks += new TestFramework("utest.runner.Framework")

Global / scalaVersion := "2.13.3"

val ourOptions = scalacOptions ~= (_.filterNot(o => Set(
  "-Ywarn-unused:imports",
  "-Wunused:imports",
  "-Ywarn-unused:params",
  "-Wunused:params",
  "-Ywarn-unused:implicits",
  "-Wunused:implicits"
)(o)))

lazy val core = (project in file("core"))
  .settings(
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-parse" % "0.0.1",
      "com.chuusai" %% "shapeless" % "2.3.3",
      "com.lihaoyi" %% "utest" % "0.7.5" % Test
    ),
    ourOptions,
    addCompilerPlugin("org.typelevel" %% "kind-projector" % "0.11.0" cross CrossVersion.full)
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