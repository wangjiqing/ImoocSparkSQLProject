package com.imooc.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * parquet文件操作（Java）
 */
public class ParquetAppForJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetAppForJava").master("local[2]")
                .getOrCreate();

        Dataset<Row> df = spark.read().load(args[0]).toDF();
        df.select("name", "favorite_color").write().format("json").save(args[1]);

        spark.stop();
        spark.close();
    }
}
