package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * Parquet文件操作（Scala）
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DateFrameCase").master("local[2]").getOrCreate()
    val rdd = spark.read.load("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\users.parquet")

    val studentDF = rdd.toDF()

//    studentDF.printSchema()
//    studentDF.show()


    studentDF.select("name", "favorite_color").write.format("json").save("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\users.json")

    spark.stop()
    spark.close()
  }
}
