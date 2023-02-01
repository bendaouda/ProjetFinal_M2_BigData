import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import org.apache.poi.ss.usermodel._

import java.io.FileOutputStream
object RequetesHiveTable extends App  {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("kafka-to-hdfs")
    .enableHiveSupport()
    .getOrCreate()

  //Question 1
  val dQ1 = spark.sql("SELECT count(distinct lien_societe) as nb_societe FROM hivedb.t_tours")
  exportToExcel(dQ1,"./Export/Question_1.xlsx","Q1","Nombre de sociétés différentes présentes dans le dataframe toursDf")

  //Question 2
  val dQ2 = spark.sql("SELECT count(distinct lien) as nb_societe FROM hivedb.t_societe")
  exportToExcel(dQ2, "./Export/Question_2.xlsx", "Q2", "Nombre de sociétés différentes présentes dans le dataframe societesDf")

  // Question 3 : variable "lien"

  //Question 4
  val dQ4 = spark.sql("SELECT count(distinct lien_societe) as nb_societe FROM hivedb.t_tours" +
    " where lien_societe NOT IN (SELECT distinct lien from hivedb.t_societe)")
  exportToExcel(dQ4, "./Export/Question_4.xlsx", "Q4", "Nombre de sociétés dans le dataframe toursDf qui ne sont pas présentes dans le dataframe societesDf")


//Exporter sous format CSV !!!


  def exportToExcel(dfram :DataFrame, fileName: String, sheetName:String, title:String) {

    val workbook = new XSSFWorkbook()
    val sheet = workbook.createSheet(sheetName)

    val row = sheet.createRow(0)
    val cell = row.createCell(0)
    cell.setCellValue(title)

    val headers = dfram.columns
    val headerRow = sheet.createRow(1)
    for (i <- headers.indices) {
      headerRow.createCell(i).setCellValue(headers(i))
    }

    val data = dfram.collect()
    for (i <- data.indices) {
      val dataRow = sheet.createRow(i + 2)
      for (j <- headers.indices) {
        dataRow.createCell(j).setCellValue(data(i)(j).toString)
      }
    }

    val fileOut = new FileOutputStream(fileName)
    workbook.write(fileOut)
    fileOut.close()
    workbook.close()
  }
}
