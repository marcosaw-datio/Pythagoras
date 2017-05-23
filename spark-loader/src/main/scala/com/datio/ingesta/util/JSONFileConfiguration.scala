package com.datio.ingesta.util

import scala.io.Source._
import scala.util.parsing.json._

/**
  * Created by marcos on 18/05/17.
  */
object JSONFileConfiguration {

  /**
    * Method to make dynamic sql from json configuration file
    * in order to estract only the fields needed
    *
    * NOTE: change implementation by HOCON
    * @param pathJson json in String type
    * @return String with created sql
    */
  @Deprecated
  def createSQLFromJSON(pathJson: String): String = {
    // String builder to build SQL
    val sbSQL = new StringBuilder("SELECT ")

    // Get json from file
    val jsonFile = fromFile(pathJson)


    // JSON parse
    val parsedJson = JSON.parseFull(jsonFile.mkString).get.asInstanceOf[Map[String, Any]]
    if(parsedJson.asInstanceOf[List[String]].contains("only") ) {
      val listOnly = parsedJson("only").asInstanceOf[List[String]]
      listOnly.foreach(sbSQL ++= _ + ", ")
      sbSQL.deleteCharAt(sbSQL.lastIndexOf(","))
    }

    sbSQL.append(" FROM tempTableName ")
    sbSQL.mkString
  }


  /**
    * Method to create Configurator object from json file in local filesystem
    *
    * @param stringJson json in string type
    * @return Configurator type
    */
  def createConfigurator(stringJson: String): Configurator = {
    // TODO: json elements validation
    val parsedJson = JSON.parseFull(stringJson)
    val sql = new StringBuilder
    val appName = parsedJson.get.asInstanceOf[Map[String, Any]]("appName").asInstanceOf[String]
    val sparkMaster = parsedJson.get.asInstanceOf[Map[String, Any]]("sparkMaster").asInstanceOf[String]
    val csvFile = parsedJson.get.asInstanceOf[Map[String, Any]]("fileToLoad").asInstanceOf[String]
    if(parsedJson.get.asInstanceOf[Map[String, Any]]("sqlOptions").asInstanceOf[Map[String,Any]].contains("extractOnly")) {
      sql.append(createSQLFromList(parsedJson.get
        .asInstanceOf[Map[String, Any]]("sqlOptions")
        .asInstanceOf[Map[String, Any]]("extractOnly")
        .asInstanceOf[List[String]]))
    } else {
      sql.append("n")
    }

    Configurator(appName , sparkMaster, csvFile, sql.mkString )

  }

  def createSQLFromList(list: List[String]): String = {

    // TODO: validate sql and clean sqlinject
    val sbSQL = new StringBuilder("SELECT ")
    list.foreach(sbSQL ++= _ + ", ")
    sbSQL.deleteCharAt(sbSQL.lastIndexOf(","))
    sbSQL.append(" FROM tempTableName ")
    sbSQL.mkString
  }


  /**
    *
    * @param appName
    * @param sparkMaster
    * @param csvFile
    * @param sql
    */
  case class Configurator(appName: String, sparkMaster: String, csvFile: String, sql: String) // TODO: complete all parameters
}

// 192.168.1.170