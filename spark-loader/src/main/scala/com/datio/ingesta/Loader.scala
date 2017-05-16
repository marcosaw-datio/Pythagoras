package com.datio.ingesta

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

/**
  * Created by marcos on 5/05/17.
  */
object Loader {

  def main(args: Array[String]): Unit = {

    val orig = args(0)
    val dest = args(1)

    // TODO: eliminate hardcoded variables
    val conf = new SparkConf().setAppName("hdfs loader").setMaster("spark://marcos-Inspiron-5567:7077")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    //val logFile = sc.textFile("hdfs://192.168.1.157:9000/users/marcos/mocks/".concat(orig))
    val csvFile = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("hdfs://192.168.1.157:9000/users/marcos/mocks/bancomer".concat(orig))

    csvFile.describe()
    // val apollo = logFile.filter(_.contains("apollo"))
    csvFile.show(10)
    println(">>>>>> cuenta df original")
    csvFile.count()
    val csvSelected = csvFile.filter(csvFile("last_name").contains("A"))
    println(">>>>>> Resultado del filter")
    csvSelected.show(10)
    println(">>>>>> cuenta df filter")
    csvFile.count()
    csvSelected.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://192.168.1.157:9000/users/marcos/spark_results/selected_data2.csv")
      //.save("~/selected_data.csv")
    // apollo.saveAsTextFile("hdfs://192.168.1.157:9000/users/marcos/spark_results/".concat(dest))
    println("se termina el trabajo con éxito")
    // sc.stop()
  }
}
