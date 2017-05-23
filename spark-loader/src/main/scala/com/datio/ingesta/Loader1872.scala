package com.datio.ingesta

import scala.io.Source._

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import com.datio.ingesta.util.NumberUtils.numberFormatter
import com.databricks.spark.avro._
import com.datio.ingesta.util.JSONFileConfiguration.createConfigurator
//import org.apache.spark.sql.types.DoubleType

/**
  * Created by marcos on 9/05/17.
  */
object Loader1872 {

  def main(args: Array[String]): Unit = {

    print("############################# version 0.05")


    // obtain arguments
    // TODO: validate args(0) (json file path in local or hdfs directory)
    val sJson = fromFile(args(0)) // path to json configurator
    val configurator = createConfigurator(sJson.mkString)

    val appName = configurator.appName
    val sparkMaster = configurator.sparkMaster
    val fileToLoad = configurator.csvFile
    val sql = configurator.sql


    // conf generate
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMaster)

    // SparkContext with conf
    val sc = new SparkContext(conf)

    // SQLContext with sc
    val sqlContext = new SQLContext(sc)

    // load csv file
    // TODO: complete json configurator to include this parameters
    val csv1872 = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "?")
      .option("treatEmptyValuesAsNulls", "true")
      .load("hdfs://192.168.1.170:8020/user/tdatuser/landingzone/".concat(fileToLoad)) // TODO: configure default hdfs landing zone in json


    // register udf for convert Double with a DecimalFormat if needed
//     sqlContext.udf.register("nf", numberFormatter _)

    // withColum example:
//    val csv1872select2 = csv1872
//      .withColumn("IM_SALDO", csv1872("IM_SALDO").cast(DoubleType))
//      .withColumn("NB_OPERACION", csv1872("NB_OPERACION"))

    // write to raw
    csv1872.write.avro("hdfs://192.168.1.170:8020/raw")

    // write to master
    if(!sql.equals("n")) {
      // register table
      csv1872.registerTempTable("tempTableName") // anterior bancomer1872 -> tempTableName
      val csv1872select = sqlContext.sql(sql)
      csv1872select.write.parquet("hdfs://192.168.1.170:8020/master")
    } else {
      csv1872.write.parquet("hdfs://192.168.1.170:8020/master")
    }

  }

}
