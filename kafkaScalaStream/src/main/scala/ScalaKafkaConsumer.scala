package org.kafkaScalaStream


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import java.util.Properties
import java.time.Duration
import scala.collection.JavaConverters.seqAsJavaListConverter

object ConsumerPlayground extends App {

  val consumerProperties = new Properties()
  consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer")
  consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

  val kafkaConsumerpays = new KafkaConsumer[String, String](consumerProperties)
  kafkaConsumerpays.subscribe(List("pays_new").asJava)

  /*
  println("key |Message")
  while (true){
    val polledRecords : ConsumerRecords[String,String] = kafkaConsumerpays.poll(Duration.ofMillis(100))
    if(!polledRecords.isEmpty){
      println(s"polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext){
        val record = recordIterator.next()
        println(s"| ${record.key()} | ${record.value()}")
      }
    }
  }

 */

  val topics = List("pays_topic", "societe_topic", "carto_topic", "tours_topic")


  val spark = SparkSession
    .builder()
    .master("local")
    .appName("kafka-to-hdfs")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  //write topics into hdfs (parquet format)

  for (top <- topics) {
    if (top == "pays_topic") {

      val PaysDF = spark
        .read
        .format("kafka")
        .option("Kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", top)
        .load()
        .selectExpr("CAST (Key AS STRING)","CAST(value AS STRING)")

      val newNames = Seq("Key", "Pays")
      val PaysDF_maj = PaysDF.toDF(newNames: _*)
      PaysDF_maj.write.mode("overwrite").format("csv").option("header", "true")
        .save(s"hdfs://Localhost:9000/user/pays/pays.csv")

      //Save DF into Hive tables
      spark.sql("CREATE DATABASE IF NOT EXISTS hivedb")
      spark.sql(s"CREATE TABLE IF NOT EXISTS hivedb.param_pays (Key string, Pays string) STORED AS TEXTFILE LOCATION 'hdfs://Localhost:9000/user/pays/pays.csv'")
      PaysDF_maj.write.mode("overwrite").format("csv").saveAsTable("hivedb.param_pays")


    }else if (top == "carto_topic") {

      val CartoDF = spark
        .read
        .format("kafka")
        .option("Kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", top)
        .load()
        .selectExpr("CAST (Key AS STRING)", "CAST(value AS STRING)")
      //CartoDF.show()
      val splitValue=split($"value", ",")
      val newCartoDf = CartoDF.select($"Key", splitValue.getItem(0).as("liste_categorie"),
                                          splitValue.getItem(1).as("automobile_et_sports"),
        splitValue.getItem(2).as("blancs"),
        splitValue.getItem(3).as("technologies_propres_ou_semi_conducteurs"),
        splitValue.getItem(4).as("divertissement"),
        splitValue.getItem(5).as("sante"),
        splitValue.getItem(6).as("fabrication"),
        splitValue.getItem(7).as("media"),
        splitValue.getItem(8).as("recherche_et_messagerie"),
        splitValue.getItem(9).as("autres"),
        splitValue.getItem(10).as("social"),
        splitValue.getItem(11).as("finance"),
        splitValue.getItem(12).as("analytique"),
        splitValue.getItem(13).as("publicite"))

      newCartoDf.show()

      newCartoDf.write.mode("overwrite").format("csv").option("delimiter",",").option("header", "true")
        .save(s"hdfs://Localhost:9000/user/carto/carto.csv")

      //Save DF into Hive tables
      spark.sql("CREATE DATABASE IF NOT EXISTS hivedb")
      spark.sql(s"CREATE TABLE IF NOT EXISTS hivedb.t_carto (Key string, " +
        s"liste_categorie string," +
        s"automobile_et_sports string," +
        s"blancs string," +
        s"technologies_propres_ou_semi_conducteurs string," +
        s"divertissement string," +
        s"sante string," +
        s"fabrication string," +
        s"media string," +
        s"recherche_et_messagerie string," +
        s" autres string," +
        s"social string," +
        s"finance string," +
        s"analytique string," +
        s"publicite string ) " +
        s"STORED AS TEXTFILE LOCATION 'hdfs://Localhost:9000/user/carto/carto.csv'")
      newCartoDf.write.mode("overwrite").format("csv").saveAsTable("hivedb.t_carto")

    } else if (top == "societe_topic") {

      val SocieteDF = spark
        .read
        .format("kafka")
        .option("Kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", top)
        .load()
        .selectExpr("CAST (Key AS STRING)", "CAST(value AS STRING)")

      //SocieteDF.show()


      val splitValue = split($"value", "\t")
      val newSocieteDf = SocieteDF.select($"Key", splitValue.getItem(0).as("lien"),
        splitValue.getItem(1).as("nom"),
        splitValue.getItem(2).as("url"),
        splitValue.getItem(3).as("liste_categorie"),
        splitValue.getItem(4).as("statut"),
        splitValue.getItem(5).as("code_pays"),
        splitValue.getItem(6).as("code_etat"),
        splitValue.getItem(7).as("region"),
        splitValue.getItem(8).as("ville"),
        splitValue.getItem(9).as("fondee_en"))

      newSocieteDf.show()

      newSocieteDf.write.mode("overwrite").format("csv").option("delimiter", "\t").option("header", "true")
        .save(s"hdfs://Localhost:9000/user/societe/societe.csv")

      //Save DF into Hive tables
      spark.sql("CREATE DATABASE IF NOT EXISTS hivedb")
      spark.sql(s"CREATE TABLE IF NOT EXISTS hivedb.t_societe (Key string, " +
        s"nom string," +
        s"url string," +
        s"liste_categorie string," +
        s"statut string," +
        s"code_pays string," +
        s"code_etat string," +
        s"region string," +
        s"ville string," +
        s"fondee_en string) " +
        s"STORED AS TEXTFILE LOCATION 'hdfs://Localhost:9000/user/societe/societe.csv'")
      newSocieteDf.write.mode("overwrite").format("csv").saveAsTable("hivedb.t_societe")

    } else if (top == "tours_topic") {

      val ToursDF = spark
        .read
        .format("kafka")
        .option("Kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", top)
        .load()
        .selectExpr("CAST (Key AS STRING)", "CAST(value AS STRING)")

      //ToursDF.show()

      val splitValue = split($"value", ",")
      val newToursDf = ToursDF.select($"Key", splitValue.getItem(0).as("lien_societe"),
        splitValue.getItem(1).as("lien_tour_investissement"),
        splitValue.getItem(2).as("type_tour_investissement"),
        splitValue.getItem(3).as("code_tour_investissement"),
        splitValue.getItem(4).as("investi_en"),
        splitValue.getItem(5).as("montant_investi_en_euro"))

      newToursDf.show()

      newToursDf.write.mode("overwrite").format("csv").option("delimiter", ",").option("header", "true")
        .save(s"hdfs://Localhost:9000/user/tours/tours.csv")

      //Save DF into Hive tables
      spark.sql("CREATE DATABASE IF NOT EXISTS hivedb")
      spark.sql(s"CREATE TABLE IF NOT EXISTS hivedb.t_tours (Key string, " +
        s"lien_societe string," +
        s"lien_tour_investissement string," +
        s"type_tour_investissement string," +
        s"code_tour_investissement string," +
        s"investi_en string," +
        s"montant_investi_en_euro string) " +
        s"STORED AS TEXTFILE LOCATION 'hdfs://Localhost:9000/user/tours/tours.csv'")
      newToursDf.write.mode("overwrite").format("csv").saveAsTable("hivedb.t_tours")

    }



  }
}