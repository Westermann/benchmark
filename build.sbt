name         := "Bench Test Spark App"
scalaVersion := "2.12.6"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0"
)
resolvers += Resolver.mavenLocal
