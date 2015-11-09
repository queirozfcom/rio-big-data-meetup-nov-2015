name := "rio-big-data-meetup-nov-2015"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided"
libraryDependencies += "joda-time" % "joda-time" % "2.8.2"


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

    