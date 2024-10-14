package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HolaArchivos {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Hola Archivos")
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> peopleCSV = spark
                .read()
                .option("sep", ";")
                .option("header", "true")
                .csv("src/main/resources/people.csv");

        peopleCSV.printSchema();
        peopleCSV.show();

        jsc.close();
        spark.close();
    }

}
