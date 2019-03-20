package com.imooc.spark

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用（Scala版本）
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    // 1) 创建响应的Context
    val sparkConf = new SparkConf()

    // 如果跑在Spark集群上时，将下面改行代码注释掉
    //    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    // 2) 进行相关处理：json
    hiveContext.table("emp").show

    // 3) 关闭资源
    sc.stop()
  }
}
