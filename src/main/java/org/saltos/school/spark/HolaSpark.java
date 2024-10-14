package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HolaSpark {

    static String cambiarAMayusculas(String s) {
        return s.toUpperCase();
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Hola Spark")
                .config("spark.master", "local")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");
        List<String> nombresEnJava = Arrays.asList("Carlos", "Pepe", "Anna", "Paul");
        JavaRDD<String> nombresEnSpark = jsc.parallelize(nombresEnJava);
        System.out.println("Nombres en Spark original");
        nombresEnSpark.foreach(nombre -> System.out.println(nombre));
        JavaRDD<String> nombresEnMayusculasRDD = nombresEnSpark.map(nombre -> nombre.toUpperCase());
        JavaRDD<String> nombresEnMayusculasRDD2 = nombresEnSpark.map(String::toUpperCase);
        JavaRDD<String> nombresEnMayusculasRDD3 =nombresEnSpark.map(nombre -> {
           String nombreEnMayusculas = nombre.toUpperCase();
           return nombreEnMayusculas;
        });
        JavaRDD<String> nombresEnMayusculasRDD4 = nombresEnSpark.map(HolaSpark::cambiarAMayusculas);
        System.out.println("Nombres en mayusculas");
        nombresEnMayusculasRDD.foreach(nombre -> System.out.println("[" + nombre + "]"));
        System.out.println("Nombres en Spark original");
        nombresEnSpark.foreach(nombre -> System.out.println(nombre));

        JavaRDD<Integer> numerosRDD = nombresEnSpark.map(nombre -> nombre.length());
        JavaRDD<Integer> numerosRDD2 = nombresEnSpark.map(String::length);

        numerosRDD.foreach(numero -> System.out.println("numero: " + numero));
        long numeroDeItems = numerosRDD.count();
        System.out.println("El numero de items es: " + numeroDeItems);

    }

}
