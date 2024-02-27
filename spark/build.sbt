import Dependencies.EmrRelease

name := "FederatedQueryExecutor"

Test / parallelExecution := false
Test / javaOptions ++= Seq("-XX:+UseG1GC")

version := "0.2"

scalaVersion := "2.12.15"

val emrRelease = EmrRelease.`Emr-6.15.0`
val awsSdkVersion = emrRelease.awsSdk
val sparkVersion = emrRelease.spark

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-avro" % sparkVersion % Provided,
  "com.amazonaws" % "aws-java-sdk" % awsSdkVersion % Provided,
  "com.typesafe" % "config" % "1.4.2",
  "com.iheart" %% "ficus" % "1.5.2",
  "mysql" % "mysql-connector-java" % "8.0.29",
  "org.apache.parquet" % "parquet-avro" % "1.12.2",
  "org.json4s" %% "json4s-native" % "3.5.5",

  "org.scalatest" %% "scalatest" % "3.2.10" % Test,
  "com.github.mrpowers" %% "spark-fast-tests" % "0.23.0" % Test
)

//for a single 'fat' jar
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _                             => MergeStrategy.first
}

/** show any deprecation warnings fully */
scalacOptions ++= Seq("-deprecation", "-feature")

lazy val root = (project in file("."))
  .settings(
    assembly /assemblyJarName := "FederatedQueryExecutor-" + version.value + "_emr-" + emrRelease.emr + ".jar",
    assembly / test := {}
  )
