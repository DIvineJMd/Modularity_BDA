name := "my-scala-project"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-graphx" % "3.2.0",
  "org.json4s" %% "json4s-jackson" % "3.7.0-M11"
)