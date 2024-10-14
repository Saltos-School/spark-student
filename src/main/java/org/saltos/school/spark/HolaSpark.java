package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HolaSpark {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Hola Spark")
                .config("spark.master", "local")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<String> nombresEnJava = Arrays.asList("Carlos", "Pepe", "Anna", "Paul");
        JavaRDD<String> nombresEnSpark = jsc.parallelize(nombresEnJava);
        System.out.println("Nombres en Spark original");
        nombresEnSpark.foreach(nombre -> System.out.println(nombre));
        JavaRDD<String> nombresEnMayusculasRDD = nombresEnSpark.map(nombre -> nombre.toUpperCase());
        System.out.println("Nombres en mayusculas");
        nombresEnMayusculasRDD.foreach(nombre -> System.out.println(nombre));
        System.out.println("Nombres en Spark original");
        nombresEnSpark.foreach(nombre -> System.out.println(nombre));
    }

}
