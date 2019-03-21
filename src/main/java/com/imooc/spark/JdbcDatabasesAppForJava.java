package com.imooc.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 *  JDBC To Other Databases (Java)
 */
public class JdbcDatabasesAppForJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("JdbcDatabasesAppForJava").master("local[2]").getOrCreate();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "chang");

        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.177.136:3306/sparksql?createDatabaseIfNotExist=true")
                .option("dbtable", "sparksql.TBLS")
                .option("user", "root")
                .option("password", "chang")
                .load();

//        jdbcDF.show();


        jdbcDF.write()
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .save();

        Dataset<Row> jdbcDF2 = spark.read()
                .jdbc("jdbc:mysql://192.168.177.136:3306/sparksql?createDatabaseIfNotExist=true", "sparksql.TBLS", connectionProperties);

        jdbcDF.write()
                .jdbc("jdbc:mysql://192.168.177.136:3306/sparksql_m?createDatabaseIfNotExist=true", "sparksql_m.TBLS", connectionProperties);

        spark.stop();
        spark.close();
    }
}
