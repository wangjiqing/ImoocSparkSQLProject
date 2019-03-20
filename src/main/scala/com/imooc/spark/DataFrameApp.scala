package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame API 基本操作(Scala)
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val sparkSession =  SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    val peopleDataFrame = sparkSession.read.format("json").load("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\people.json")
    // 输出对应的Schema信息
    peopleDataFrame.printSchema()
    // 输出数据集的20条
    peopleDataFrame.show()
    // 查询某一列所有的数据
    peopleDataFrame.select("name").show()
    // 查询某几列所有数据，并操作列数据
    peopleDataFrame.select(peopleDataFrame.col("name"), (peopleDataFrame.col("age") + 10).as("ageV2")).show()
    // 根据某一列的值做过滤操作 select name, (age + 10) as ageV2 from table
    peopleDataFrame.filter(peopleDataFrame.col("age") > 19).show()
    // 根据某一列进行分组，然后再进行聚合操作 select age, count(1) from table group by age
    peopleDataFrame.groupBy("age").count().show()
    sparkSession.stop()
    sparkSession.close()
  }
}
