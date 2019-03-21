package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * Hive Join MySQL demo (Scala)
  */
object HiveMySqlApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveMySqlApp").master("local[2]").getOrCreate()

    /* 加载Hive表数据 */
    val hiveDF = spark.table("emp")

    /* 加载MySQL表数据 */
    val mysqlDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.177.136:3306/sparksql?createDatabaseIfNotExist=true")
      .option("dbtable", "sparksql.TBLS")
      .option("user", "root")
      .option("password", "chang")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    /* JOIN */
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))

    resultDF.show(30, false)

    resultDF.select(hiveDF.col("empno"), hiveDF.col("ename"), mysqlDF.col("deptno"), mysqlDF.col("dname")).show()

    spark.stop()
    spark.close()
  }
}
