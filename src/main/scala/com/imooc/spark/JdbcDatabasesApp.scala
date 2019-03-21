package com.imooc.spark

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * JDBC To Other Databases (Scala)
  */
object JdbcDatabasesApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JdbcDatabasesApp").master("local[2]").getOrCreate()

//    readMySql1(spark)
    val jdbcDF = readMySql2(spark)

//    jdbcDF.show()

//    writeMySql1(jdbcDF)

    writeMySql2(jdbcDF)

    spark.stop()
    spark.close()
  }

  private def readMySql1(spark: SparkSession): DataFrame = {
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.177.136:3306/sparksql?createDatabaseIfNotExist=true")
      .option("dbtable", "sparksql.TBLS")
      .option("user", "root")
      .option("password", "chang")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    jdbcDF
  }

  def readMySql2(spark: SparkSession): DataFrame = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "chang")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://192.168.177.136:3306/sparksql?createDatabaseIfNotExist=true", "sparksql.TBLS", connectionProperties)

    jdbcDF2
  }

  def writeMySql1(jdbcDF: DataFrame): Unit = {
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.177.136:3306/sparksql_m?createDatabaseIfNotExist=true")
      .option("dbtable", "sparksql_m.TBLS")
      .option("user", "root")
      .option("password", "chang")
      .option("driver", "com.mysql.jdbc.Driver")
      .save()
  }

  def writeMySql2(jdbcDF2: DataFrame): Unit = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "chang")
    jdbcDF2.write
      .jdbc("jdbc:mysql://192.168.177.136:3306/sparksql_m?createDatabaseIfNotExist=true", "sparksql_m.TBLS", connectionProperties)
  }
}
