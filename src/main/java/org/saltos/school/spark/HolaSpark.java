package org.saltos.school.spark;

import org.apache.spark.api.java.JavaDoubleRDD;
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
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        List<String> numerosComoCadenasEnJava = Arrays.asList("1", "5", "-20", "40");

        jsc.parallelize(numerosComoCadenasEnJava).mapToDouble(n -> {
            try {
                return Integer.parseInt(n);
            } catch (NumberFormatException e) {
                return 0;
            }
        }).sum();

        jsc.parallelize(numerosComoCadenasEnJava).mapToDouble(n -> Double.parseDouble(n)).sum();

        JavaRDD<String> numeroConCadenasEnSpark = jsc.parallelize(numerosComoCadenasEnJava);
        JavaDoubleRDD numoersDoublesEnSpark = numeroConCadenasEnSpark.mapToDouble(n -> Double.parseDouble(n));
        double total3 = numoersDoublesEnSpark.sum();

        List<String> nombresEnJava = Arrays.asList("Carlos", "Pepe", "Anna", "Paul");
        JavaRDD<String> nombresEnSpark = jsc.parallelize(nombresEnJava);
        System.out.println("Nombres en Spark original");
        nombresEnSpark.foreach(nombre -> System.out.println(nombre));
        JavaRDD<String> nombresEnMayusculasRDD = nombresEnSpark.map(nombre -> nombre.toUpperCase());
        JavaRDD<String> nombresEnMayusculasRDD2 = nombresEnSpark.map(String::toUpperCase);
        JavaRDD<String> nombresEnMayusculasRDD3 = nombresEnSpark.map(nombre -> {
           String nombreEnMayusculas = nombre.toUpperCase();
           return nombreEnMayusculas;
        });
        JavaRDD<String> nombresEnMayusculasRDD4 = nombresEnSpark.map(HolaSpark::cambiarAMayusculas);
        System.out.println("Nombres en mayusculas");
        nombresEnMayusculasRDD.foreach(nombre -> System.out.println("[" + nombre + "]"));
        System.out.println("Nombres en Spark original");
        //nombresEnSpark.foreach(System.out::println);
        nombresEnSpark.foreach(nombre -> System.out.println(nombre));

        JavaRDD<Integer> numerosRDD = nombresEnSpark.map(nombre -> nombre.length());
        JavaRDD<Integer> numerosRDD2 = nombresEnSpark.map(String::length);

        double total2 = numerosRDD.mapToDouble(numero -> numero).sum();
        System.out.println("Total suma de numeros es: " + total2);

        numerosRDD.foreach(numero -> System.out.println("numero: " + numero));
        long numeroDeItems = numerosRDD.count();
        System.out.println("El numero de items es: " + numeroDeItems);

        List<String> nombresEnJava2 = nombresEnSpark.collect();
        System.out.println("Collect de Java");
        nombresEnJava2.forEach(nombre -> System.out.println(nombre));

        JavaRDD<Double> numerosDoblesRDD = numerosRDD.map(numero -> numero * 2.0);
        JavaDoubleRDD numerosDoubleRDD = numerosRDD.mapToDouble(numero -> numero * 2.0);

        //numerosDoblesRDD.sum();
        double total = numerosDoubleRDD.sum();
        //numerosDoblesRDD.max();
        double maximo = numerosDoubleRDD.max();

        System.out.println("El total es: " + total);
        System.out.println("El máximo es: " + maximo);

        jsc.close();
        spark.close();

    }

}
