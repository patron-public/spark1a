name := "spark1a"

version := "1.0"

scalaVersion := "2.10.5"

mainClass in Compile := Some("com.epam.spark.AvgBytesCounter")

libraryDependencies ++= Seq(
  ("org.apache.spark" % "spark-core_2.10" % "1.6.0").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog")
    exclude("com.google.guava", "guava")
)

