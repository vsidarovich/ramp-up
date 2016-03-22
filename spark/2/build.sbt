name := "shiffer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq("org.pcap4j" % "pcap4j-core" % "1.6.1",
                            "org.pcap4j" % "pcap4j-packetfactory-static" % "1.6.1",
                            "org.apache.spark" %% "spark-core" % "1.3.1",
                            "org.apache.spark" %%  "spark-sql" % "1.3.1",
                            "org.apache.spark" %% "spark-streaming" % "1.3.1",
                            "org.apache.spark" %% "spark-streaming-kafka" % "1.3.1",
                            "org.apache.kafka" %% "kafka" % "0.8.2.2"
                                exclude("javax.jms", "jms")
                                exclude("com.sun.jdmk", "jmxtools")
                                exclude("com.sun.jmx", "jmxri")
                                exclude("org.slf4j", "slf4j-simple")
                                exclude("log4j", "log4j")
                                exclude("org.apache.zookeeper", "zookeeper")
                                exclude("com.101tec", "zkclient"),
                            "org.apache.spark" %% "spark-hive" % "1.3.1",
                            "org.apache.hbase" % "hbase-common" % "1.1.2"
                              excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"),
                              ExclusionRule(organization = "org.mortbay.jetty", name="jetty"),
                              ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5")),
                            "org.apache.hbase" % "hbase-client" % "1.1.2"
                              excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"),
                              ExclusionRule(organization = "org.mortbay.jetty", name="jetty"),
                              ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5")),
                            "org.apache.hbase" % "hbase-server" % "1.1.2"
                              excludeAll(ExclusionRule(organization = "javax.servlet", name="javax.servlet-api"),
                              ExclusionRule(organization = "org.mortbay.jetty", name="jetty"),
                              ExclusionRule(organization = "org.mortbay.jetty", name="servlet-api-2.5"))
)

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Akka repository" at "http://repo.akka.io/releases/"