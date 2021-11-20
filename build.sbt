name := "com/chatbot_spark"
version := "0.1"
scalaVersion := "2.12.5"
val sparkVersion = "3.2.0"

mainClass := Some("com.chatbot_spark.main")

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion,

  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.17" ,
  "com.typesafe.akka" %% "akka-stream" % "2.6.17",
  "com.typesafe.akka" %% "akka-http" % "10.2.7",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.2.7",
  "ch.megard" %% "akka-http-cors" % "1.1.2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}