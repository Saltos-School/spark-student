package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HolaDataset {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(HolaDataset.class.getSimpleName())
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> employeesDF = spark.read().json("src/main/resources/employees.json");
        employeesDF.printSchema();
        employeesDF.show();

        jsc.close();
        spark.close();
    }

}
