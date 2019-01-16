lazy val commonSettings = Seq(
    organization := "com.westermann",
    version := "0.0.1",
    scalaVersion := "2.12.6"
)

lazy val root = (project in file("."))
.settings(
    commonSettings,
    name := "Bench Test Spark App",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.4.0",
      "org.apache.spark" %% "spark-sql" % "2.4.0"
    )
)

resolvers += Resolver.mavenLocal
fork in run := true
