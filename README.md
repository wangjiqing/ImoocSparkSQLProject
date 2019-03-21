Spark SQL demo for spark-2.1.0-bin-2.6.0-cdh5.7.0

1. SQLContext的使用（1.6.x）

    Scala  | Java
    ------------- | -------------
    [SQLContextApp.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/SQLContextApp.scala)  | [SQLContextAPPForJava.java](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/java/com/imooc/spark/SQLContextAPPForJava.java)
 
2. HiveContext的使用（1.6.x）

    Scala  | Java
    ------------- | -------------
    [HiveContextApp.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/HiveContextApp.scala)  | [HiveContextAppForJava.java](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/java/com/imooc/spark/HiveContextAppForJava.java)
 
3. SparkSession的使用

   Scala  | Java
   ------------- | -------------
   [SparkSessionApp.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/SparkSessionApp.scala)  | [SparkSessionAppForJava.java](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/java/com/imooc/spark/SparkSessionAppForJava.java)
   
4. thriftserver&beeline的使用 for JDBC方式编程访问

    Scala  | Java
   ------------- | -------------
   [SparkSQLThriftServerApp.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/SparkSQLThriftServerApp.scala)  | [SparkSQLThriftServerAppForJava.java](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/java/com/imooc/spark/SparkSQLThriftServerAppForJava.java)
     
5. Spark SQL 之 DataFrame API 操作案例实战

     Scala  | Java
     ------------- | -------------
     [DataFrameApp.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/DataFrameApp.scala)  | [DataFrameAppForJava.java](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/java/com/imooc/sparkDataFrameAppForJava.java)
     
6. DataFrame 与 RDD 互操作之一：使用反射推断Schema（事先知道字段、字段类型）---- 优先选择
7. DataFrame 与 RDD 互操作之二：以编程方式指定Schema（事先不知道字段、字段类型）--- 其次选择

    Scala  | Java
    ------------- | -------------
    [DataFrameRDDApp.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/DataFrameRDDApp.scala)  | [DataFrameRDDAppForJava.java](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/java/com/imooc/spark/DataFrameRDDAppForJava.java)
   
8. DataFrame API 操作案例实战

    [DateFrameCase.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/DateFrameCase.scala)
    
9. 操作Parquet文件数据（Spark SQL不指定格式的前提下模式选择处理parquet文件，执行需要HDFS服务）
    
    Scala  | Java
    ------------- | -------------
    [ParquetApp.scala](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/scala/com/imooc/spark/ParquetApp.scala)  | [ParquetAppForJava.java](https://github.com/wangjiqing/ImoocSparkSQLProject/tree/master/src/main/java/com/imooc/ParquetAppForJava.java)
         
    