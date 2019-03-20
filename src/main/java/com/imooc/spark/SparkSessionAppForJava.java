package com.imooc.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionAppForJava {
    public static void main(String[] args) {
        String path = args[0];

        SparkConf sparkConf = new SparkConf();
//        sparkConf.setAppName("SparkSessionAppForJava").setMaster("local[2]");

        SparkContext sparkContext = new SparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(sparkContext);

        Dataset<Row> peopleDataset = sparkSession.read().format("json").load(path);
        peopleDataset.show();

        sparkSession.stop();
        sparkSession.close();
    }
}
