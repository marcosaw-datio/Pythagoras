package com.datio.ingesta

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.datio.ingesta.util.NumberUtils.numberFormatter
import com.databricks.spark.avro._
import org.apache.spark.sql.types.DoubleType

/**
  * Created by marcos on 9/05/17.
  */
object Loader1872 {

  def main(args: Array[String]): Unit = {

    print("############################# version 0.04")

    // obtain arguments
    // TODO: validate arguments with a method or class with default implementation
    val appName = args(0)
    val sparkMaster = args(1)
    val origen = args(2) // muestra1872.csv

    // conf generate
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(sparkMaster)

    // SparkContext with conf
    val sc = new SparkContext(conf)

    // SQLContext with sc
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // load 1872
    val csv1872 = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "?")
      .load("hdfs://192.168.1.157:9000/users/marcos/mocks/bancomer/".concat(origen)) // TODO: configure default hdfs landing zone

    // make conditional
    csv1872.select(csv1872("IM_SALDO"))

    // register table
    csv1872.registerTempTable("bancomer1872")

    // register udf for clean colons in numbers
    //sqlContext.udf.register("colonClean", (sn: String) => (sn.replace(",", "")))

    // register udf for convert Double with a DecimalFormat
    sqlContext.udf.register("nf", numberFormatter _)
    //sqlContext.sql("SELECT nf(IM_SALDO) as im_saldo FROM bancomer1872").show(10)

    val csv1872select = sqlContext.sql("SELECT CD_TARJETA, " +
      "NU_BIN_TARJETA, " +
      "CD_GIRO, " +
      "IM_TRANSACCION, " +
      "CD_CANAL, " +
      "IX_CROSS_BORDER, " +
      "FH_OPERACION, " +
      "TP_POS_ENTRY_MODE1, " +
      "TP_POS_ENTRY_MODE2, " +
      "cast(TP_ECI as string), " +
      "TP_LIBRE_5, " +
      "CD_RETORNO," +
      "CD_RAZON_RET, " +
      "CD_OPERACION, " +
      "TP_MENSAJE, " +
      "CD_ADQUIRENTE, " +
      "NU_AFILIACION, " +
      "TP_CUENTA, " +
      "TP_CUENTA_DESTINO " +
      //"nf(IM_SALDO) as im_saldo " +
      "FROM bancomer1872")
    val csv1872select2 = csv1872
      .withColumn("IM_SALDO", csv1872("IM_SALDO").cast(DoubleType))
      .withColumn("NB_OPERACION", csv1872("NB_OPERACION"))

    csv1872select2.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("hdfs://192.168.1.157:9000/users/marcos/spark_results/estracto1872")

    csv1872select.write.parquet("hdfs://192.168.1.157:9000/users/marcos/spark_results/estracto1872_parquet")
    csv1872select.write.avro("hdfs://192.168.1.157:9000/users/marcos/spark_results/estracto1872_avro")

  }

}
