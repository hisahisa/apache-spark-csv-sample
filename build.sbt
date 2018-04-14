name := "spark_csv_sample"

version := "1.0.0"

scalaVersion := "2.11.2"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0",
  "org.apache.spark" % "spark-core_2.11" % "1.6.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "mysql" % "mysql-connector-java" % "5.1.46",
  "org.mortbay.jetty" % "jetty" % "6.1.26"
)
