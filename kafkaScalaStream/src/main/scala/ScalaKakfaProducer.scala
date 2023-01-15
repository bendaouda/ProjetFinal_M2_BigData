package org.kafkaScalaStream

import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.Source

object ScalaKakfaProducer {
  def main(args: Array[String]): Unit = {
    val config: Properties = new Properties()
    config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") // Enter your own bootstrap server IP:PORT
    config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config)

    // Enter your file name with path here
    val fileName = "C:\\Users\\soule\\OneDrive\\Documents\\M2 BI\\Veille Technologique - Big Data\\Projet Final BI\\ProjetFinal_M2_BigData\\datasets\\pays.csv"

    // Enter your Kafka input topic name
    val topicName = "pays_topic_bis"

    for (line <- Source.fromFile(fileName).getLines()) { // Dropping the column names
      // Extract Key
      //val key = line.split(";") {
        //0
      //}
      val rand = new scala.util.Random
      val key = rand.nextInt().toString()
      //println(key + " : " + line)
      // Prepare the record to send
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topicName, key, line)

      // Send to topic
      producer.send(record)
      println(record)
    }

    producer.flush()
    producer.close()
  }
}