
name := "wikipedia-streaming-example"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

lazy val kafkaVersion = "0.10.1.0-SNAPSHOT"

libraryDependencies += "org.apache.kafka" % "connect-runtime" % kafkaVersion

libraryDependencies += "org.apache.kafka" % "connect-api" % kafkaVersion

libraryDependencies += "org.apache.kafka" % "kafka-streams" % kafkaVersion

libraryDependencies += "org.schwering" % "irclib" % "1.10"

libraryDependencies += "org.specs2" %% "specs2-core" % "3.7.2" % "test"

cancelable in Global := true