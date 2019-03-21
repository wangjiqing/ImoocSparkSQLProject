package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * Parquet文件操作（Scala）
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {

    /* 读取parquet文件 */
    val spark = SparkSession.builder()/*.appName("DateFrameCase").master("local[2]")*/.getOrCreate()
    val rdd = spark.read.load(args(0))

    val studentDF = rdd.toDF()

//    studentDF.printSchema()
//    studentDF.show()

    // 处理完成之后以JSON的形式写入到文件
    studentDF.select("name", "favorite_color").write.format("json").save(args(1))

    spark.stop()
    spark.close()
  }
}
