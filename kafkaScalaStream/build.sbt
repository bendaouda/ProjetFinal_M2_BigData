ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "kafkaScalaStream"
  )
val sparkVersion = "3.3.1"
/*
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % "3.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.2.2" % "provided",
  "org.apache.derby" % "derby" % "10.16.1.1" % Test,
)
libraryDependencies ++= sparkDependencies
*/
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1" % "compile"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"  % "compile"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.1"

// https://mvnrepository.com/artifact/com.databricks/spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.16.0"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.30"

libraryDependencies += "org.apache.spark" %% "spark-avro" % "3.2.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.30"

libraryDependencies += "org.apache.poi" % "poi-ooxml" % "4.1.2"


/*
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.19.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.13.3"
*/