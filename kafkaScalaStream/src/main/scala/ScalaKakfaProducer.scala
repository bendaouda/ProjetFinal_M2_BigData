package org.kafkaScalaStream

import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.nio.file.{Files, Paths}

object ScalaKakfaProducer {
  def main(args: Array[String]): Unit = {
    val config: Properties = new Properties()
    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Enter your own bootstrap server IP:PORT
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

    // Enter your file name with path here
    val fileName1 = "/home/bend/Documents/M2_BI/BigData/ProjetFinal/ProjetFinal_M2_BigData/datasets/pays.csv"
    val fileName2 = "/home/bend/Documents/M2_BI/BigData/ProjetFinal/ProjetFinal_M2_BigData/datasets/cartographie.csv"
    val fileName3 = "/home/bend/Documents/M2_BI/BigData/ProjetFinal/ProjetFinal_M2_BigData/datasets/societe.txt"
    val fileName4 = "/home/bend/Documents/M2_BI/BigData/ProjetFinal/ProjetFinal_M2_BigData/datasets/tours.csv"

    // Enter your Kafka input topic name
    val topicName1 = "pays_topic"
    val topicName2 = "carto_topic"
    val topicName3 = "societe_topic"
    val topicName4 = "tours_topic"

    println("Producing PAYS")
    for (line <- scala.io.Source.fromFile(fileName1).getLines().drop(1)) {
      val rand = new scala.util.Random
      val key = rand.nextInt().toString()
      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName1, key, line)
      // Send to topic
      producer.send(record)
      println(record)
    }

    println("Producing CARTO")
    for (line <- scala.io.Source.fromFile(fileName2).getLines().drop(1)) {
      val rand = new scala.util.Random
      val key = rand.nextInt().toString()
      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName2, key, line)
      // Send to topic
      producer.send(record)
      println(record)
    }

    println("Producing SOCIETE")
    for (line <- scala.io.Source.fromFile(fileName3).getLines().drop(1)) {
      val rand = new scala.util.Random
      val key = rand.nextInt().toString()
      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName3, key, line)
      // Send to topic
      producer.send(record)
      println(record)
    }

    println("Producing TOURS")
    for (line <- scala.io.Source.fromFile(fileName4).getLines().drop(1)) {
      val rand = new scala.util.Random
      val key = rand.nextInt().toString()
      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName4, key, line)
      // Send to topic
      producer.send(record)
      println(record)
    }

    producer.flush()
    producer.close()



  }
}