package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class HolaOperaciones {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(HolaOperaciones.class.getSimpleName())
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        List<Double> numeros = Arrays.asList(1.0, 2.0, 3.0);
        JavaRDD<Double> numerosEnSpark = jsc.parallelize(numeros, 2);
        System.out.println("Numeros en spark:");
        numerosEnSpark.collect().forEach(System.out::println);

        // map (tranformación)
        JavaRDD<Double> incrementoUnoPuntoCinco = numerosEnSpark.map(n -> n + 1.5);
        System.out.println("Incremento uno punto cinco:");
        incrementoUnoPuntoCinco.collect().forEach(System.out::println);

        // flatMap (transformación)
        JavaRDD<List<Double>> porDosyPorTresLista = numerosEnSpark.map(n -> Arrays.asList(n * 2, n * 3));
        System.out.println("Por dos y por tres lista:");
        porDosyPorTresLista.collect().forEach(System.out::println);

        JavaRDD<Double> porDosyPorTres = numerosEnSpark.flatMap(n -> Arrays.asList(n * 2, n * 3).iterator());
        System.out.println("Por dos y por tres:");
        porDosyPorTres.collect().forEach(System.out::println);


        jsc.close();
        spark.close();
    }

}
