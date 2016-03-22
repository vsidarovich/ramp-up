name := "spark-sbt"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.5.1",
                            "org.apache.spark" %%  "spark-sql" % "1.5.1",
                            "org.scalatest" %% "scalatest" % "2.2.5" % "test"
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Akka repository" at "http://repo.akka.io/releases/"