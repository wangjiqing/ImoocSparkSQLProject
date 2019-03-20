package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Scala版本的Spark SQL 处理JSON数据的Demo
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    // 指定测试的json文件的路径 例如：E:\Idea-GitHub\ImoocSparkSQLProject\src\main\resources\people.json
    val path = args(0)

    // 1) 创建响应的Context
    val sparkConf = new SparkConf()

    // 如果跑在Spark集群上时，将下面改行代码注释掉
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // 2) 进行相关处理：json
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    // 3) 关闭资源
    sc.stop()
  }
}
