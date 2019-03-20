package com.imooc.spark;

import com.imooc.spark.bean.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * ataFrame与RDD的互操作（Java）
 */
public class DataFrameRDDAppForJava {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("DataFrameRDDAppForJava").setMaster("local[2]");

        SparkContext sc = new SparkContext(sparkConf);
        SparkSession sparkSession = new SparkSession(sc);

//        inferReflection(sparkSession);

        program(sparkSession);

        sparkSession.stop();
        sparkSession.close();

    }

    /**
     * 以编程方式指定Schema（Java）
     * @param sparkSession
     */
    private static void program(SparkSession sparkSession) {
        JavaRDD<String> peopleRDD = sparkSession.read()
                .textFile("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\infos.txt")
                .javaRDD();

        String schemaString = "id,name,age";
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(",")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRdd = peopleRDD.map(new Function<String, Row>() {
            public Row call(String record) {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1], attributes[2]);
            }
        });

        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowRdd, schema);

        peopleDataFrame.createOrReplaceTempView("infos");

        sparkSession.sql("select * from infos").show();
    }

    /**
     * 使用反射推断Schema(Java)
     * @param sparkSession
     */
    private static void inferReflection(SparkSession sparkSession) {
        JavaRDD<Person> personJavaRDD = sparkSession.read()
                .textFile("E:\\Idea-GitHub\\ImoocSparkSQLProject\\src\\main\\resources\\infos.txt")
                .javaRDD()
                .map(new Function<String, Person>() {
                    public Person call(String line) {
                        String[] parts = line.split(",");
                        Person person = new Person();
                        person.setId(Integer.parseInt(parts[0]));
                        person.setName(parts[1]);
                        person.setAge(Integer.parseInt(parts[2].trim()));
                        return person;
                    }
                });

        Dataset<Row> peopleDF = sparkSession.createDataFrame(personJavaRDD, Person.class);
        peopleDF.createOrReplaceTempView("infos");

        Dataset<Row> teenagersDF = sparkSession.sql("select * from infos");
        teenagersDF.show();

        teenagersDF.filter(teenagersDF.col("age").gt(30)).show();
        // DataFrame 转换成临时表，使用SQL形式访问
        sparkSession.sql("select * from infos where age > 30").show();
    }
}
