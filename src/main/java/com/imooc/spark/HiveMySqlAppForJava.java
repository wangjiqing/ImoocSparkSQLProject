package com.imooc.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hive Join MySQL demo (Java)
 */
public class HiveMySqlAppForJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("HiveMySqlAppForJava").master("local[2]").getOrCreate();
        /* 加载Hive表数据 */
        Dataset<Row> hiveDataset = spark.table("emp");
        /* 加载MySQL表数据 */
        Dataset<Row> mysqlDataset = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.177.136:3306/sparksql?createDatabaseIfNotExist=true")
                .option("dbtable", "sparksql.TBLS")
                .option("user", "root")
                .option("password", "chang")
                .load();

        Dataset<Row> resultDataset = hiveDataset.join(mysqlDataset, mysqlDataset.col("deptno").equalTo(hiveDataset.col("DEPTNO")));

        resultDataset.show();

        resultDataset.select(hiveDataset.col("deptno"), mysqlDataset.col("DEPTNO")).show();

        spark.stop();
        spark.close();
    }
}
