package org.kafkaScalaStream

import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig._

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql

object ConsumerPlayground extends App {

  // HDFS configuration
  val hdfsConf = new Configuration()
  hdfsConf.set("fs.defaultFS", "hdfs://localhost:9000")
  val hdfs = FileSystem.get(hdfsConf)

  val topicName = "pays_topic_bis"
  val consumerProperties = new Properties()

  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-2")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

  val consumer = new KafkaConsumer[String, String](consumerProperties)

  consumer.subscribe(List(topicName).asJava)


//hive start
  val spark_conf = new SparkConf().setAppName("Read-CSV").setMaster("local[*]")
  val sc = new SparkContext(spark_conf)

  val spark_session = SparkSession.builder()
    .config(spark_conf)
    .enableHiveSupport()
    .getOrCreate()

  //CSV file on hdfs

  val hdfs_file_location="hdfs://localhost:9000/user/pays/pays_topic_bis-1008310031.csv"

  //reading
  val username_df=readFromHDFS(hdfs_file_location,spark_session)
  username_df.printSchema()
  username_df.collect.foreach(println)

  def readFromHDFS(file_location: String, spark_session: SparkSession):DataFrame ={

      val username_df = spark_session.read.format("csv")
        .option("delimiter",";")
        .option("header","true")
        .option("inferShema","true")
        .load(file_location)

    username_df
  }


  spark_session.sql("CREATE DATABASE Hive_test")
  //spark_session.sql("show databases").show()

  // Hive End



  println("| Key | Message ")
  while (true) {
    val polledRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))
    if (!polledRecords.isEmpty) {
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext) {
        val record: ConsumerRecord[String, String] = recordIterator.next()
        //val csvTrip = record.value()
        //println(s"| ${record.key()} | ${record.value()} | ${record.partition()} | ${record.offset()} |")
        //println(s"| ${record.key()} | ${record.value()} |")
        //println(csvTrip)

        // Write each record to HDFS
        val path = new Path("/user/pays/"+topicName + record.key() + ".csv")
        val outputStream = hdfs.create(path)
        outputStream.write(record.value().getBytes)
        outputStream.close()

      }
    }
  }






}