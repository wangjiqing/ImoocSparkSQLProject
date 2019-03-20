package com.imooc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *   * DataFrame API 基本操作(Java)
 *   */
public class DataFrameAppForJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]").setAppName("DataFrameAppForJava");
        SparkContext sc = new SparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(sc);

        Dataset<Row> rowDataset = sparkSession.read().json("E:\\IdeaProjects\\ImoocSparkSQLProject\\src\\main\\resources\\people.json");
        // 输出对应的Schema信息
        rowDataset.printSchema();
        // 输出数据集的20条
        rowDataset.show();
        // 查询某一列所有的数据
        rowDataset.select("name").show();
        // 查询某几列所有数据，并操作列数据
        rowDataset.select(rowDataset.col("name"), (rowDataset.col("age").plus(10)).as("ageV2")).show();
        // 根据某一列的值做过滤操作 select name, (age + 10) as ageV2 from table
        rowDataset.filter(rowDataset.col("age").gt(19)).show();
        // 根据某一列进行分组，然后再进行聚合操作 select age, count(1) from table group by age
        rowDataset.groupBy("age").count().show();

        sparkSession.stop();
        sparkSession.close();
    }
}
