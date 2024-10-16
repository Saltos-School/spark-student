package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HolaSparkSql {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(HolaSparkSql.class.getSimpleName())
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> employeesDF = spark.read().json("src/main/resources/employees.json").cache();
        employeesDF.printSchema();
        employeesDF.show();

        employeesDF.createOrReplaceTempView("empleado");
        Dataset<Row> empleadosMejorPagadosDF = spark.sql("SELECT name FROM empleado WHERE salary > 3500");
        empleadosMejorPagadosDF.printSchema();
        empleadosMejorPagadosDF.show();

        employeesDF.createOrReplaceGlobalTempView("empleado");
        Dataset<Row> empleadosMejorPagadosDF2 = spark.sql("SELECT name FROM global_temp.empleado WHERE salary > 3500");
        empleadosMejorPagadosDF2.printSchema();
        empleadosMejorPagadosDF2.show();

        jsc.close();
        spark.close();
    }

}
