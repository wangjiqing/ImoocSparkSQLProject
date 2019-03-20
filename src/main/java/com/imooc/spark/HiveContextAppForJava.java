package com.imooc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * HiveContext的使用（Java版本）
 */
public class HiveContextAppForJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        HiveContext hiveContext = new HiveContext(sparkContext);

        hiveContext.table("emp").show();
    }
}
