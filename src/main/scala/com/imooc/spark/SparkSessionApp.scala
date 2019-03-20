package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * SparkSession使用Demo（Scala）
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    // 指定测试的json文件的路径 例如：E:\Idea-GitHub\ImoocSparkSQLProject\src\main\resources\people.json
    val path = args(0)

    val sparkSession = SparkSession.builder()/*.appName("SparkSessionApp").master("local[2]")*/.getOrCreate()

    val people = sparkSession.read.json(path)

    people.show()

    sparkSession.stop()
    sparkSession.close()
  }
}
