package com.imooc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Java版本的Spark SQL 处理JSON数据的Demo
 */
public class SQLContextAppForJava {
    public static void main(String[] args) {
        // 指定测试的json文件的路径 例如：E:\Idea-GitHub\ImoocSparkSQLProject\src\main\resources\people.json
        String path = args[0];
        // 1) 创建响应的Context
        SparkConf sparkConf = new SparkConf();
        // 如果跑在Spark集群上时，将下面改行代码注释掉
//        sparkConf.setAppName("SQLContextAppForJava").setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sparkContext);
        // 2) 进行相关处理：json
        Dataset<Row> peopleDataSet = sqlContext.read().format("json").load(path);
        // 显示表结构
        peopleDataSet.printSchema();
        // 显示数据
        peopleDataSet.show();

        // 3) 关闭资源
        sparkContext.close();
    }
}
