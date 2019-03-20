package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame中的实战操作
  */
object DateFrameCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DateFrameCase").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.textFile("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\student.data")

    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //
    studentDF.show(30, false)
//    studentDF.take(10).foreach(println)
    studentDF.first()
    // 查询 Email 列的数据
    studentDF.select("email").show(30, false)
    // 过滤name 为 '' 或者 name 为 'NULL' 的数据
    studentDF.filter("name='' OR name='NULL'").show()
    // 过滤name的开头字符为M的数据
    studentDF.filter("SUBSTR(name,0,1)='M'").show()

    // 列出SQL内置函数
    spark.sql("show functions").show(1000)

    // 采用默认排序（升序） 使用 name 字段排序
    studentDF.sort(studentDF("name")).show(30, false)
    // 采用降序排序 使用 name 字段排序
    studentDF.sort(studentDF("name").desc).show(30, false)
    // 采用两个字段模式排序
    studentDF.sort("name", "id").show(30, false)
    // 采用两个字段排序，一个升序，一个降序
    studentDF.sort(studentDF("name").asc, studentDF("id").desc).show(30, false)
    // 修改字段名称
    studentDF.select(studentDF("name").as("studentName")).show(30, false)
    // 联表查询
    studentDF.join(studentDF, studentDF.col("id") === studentDF.col("id")).show(30, false)

    spark.stop()
    spark.close()
  }

  case class Student(id: Int, name: String, phone: String, email: String)
}
