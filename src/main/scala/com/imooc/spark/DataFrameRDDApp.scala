package com.imooc.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


/**
  * DataFrame与RDD的互操作（Scala）
  */
object DataFrameRDDApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataFrameRDDApp")
      .master("local[2]")
      .getOrCreate()

//    inferReflection(sparkSession)

    program(sparkSession)

    sparkSession.stop()
    sparkSession.close()
  }

  /**
    * 以编程方式指定Schema（Scala）
    * @param sparkSession
    */
  def program(sparkSession: SparkSession): Unit = {
    // RDD => DataFrame
    val rdd = sparkSession
      .sparkContext
      .textFile("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\infos.txt")
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))

    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)))

    val infoDF = sparkSession.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()

    // ... 通过 DataFrame 的 API 继续操作

    sparkSession.stop()
    sparkSession.close()
  }

  /**
    * 使用反射推断Schema
    * @param sparkSession
    */
  private def inferReflection(sparkSession: SparkSession) = {
    // RDD => DataFrame
    val rdd = sparkSession
      .sparkContext
      .textFile("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\infos.txt")
    // 插入隐式转换
    import sparkSession.implicits._
    val infoDataFrame = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    infoDataFrame.show()
    infoDataFrame.filter(infoDataFrame.col("age") > 30).show()
    // DataFrame 转换成临时表，使用SQL形式访问
    infoDataFrame.createOrReplaceTempView("infos")
    sparkSession.sql("select * from infos where age > 30").show()
  }

  case class Info(id: Int, name: String, age: Int)
}
