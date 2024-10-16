package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import java.util.Collections;

public class HolaAcumuladores {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(HolaAcumuladores.class.getSimpleName())
                .config("spark.master", "local[*]")
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        LongAccumulator contador = jsc.sc().longAccumulator("contador");

        JavaRDD<Double> datos = jsc.parallelize(Collections.nCopies(1000, 1)).map(n -> Math.random());

        datos.foreach(n -> {
            contador.add(1);
        });

        System.out.println("El contador es: " + contador.value());

        jsc.close();
        spark.close();
    }

}
