package org.saltos.school.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

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

        List<Double> numeros = Arrays.asList(1.0, 2.0, 3.0, -5.0, 20.0, 10.0);
        JavaRDD<Double> numerosEnSpark = jsc.parallelize(numeros, 4).persist(StorageLevel.MEMORY_AND_DISK());
        System.out.println("Numeros en spark:");
        numerosEnSpark.collect().forEach(System.out::println);

        // map (tranformación)
        JavaRDD<Double> incrementoUnoPuntoCinco = numerosEnSpark.map(n -> n + 1.5).persist(StorageLevel.DISK_ONLY());
        System.out.println("Incremento uno punto cinco:");
        incrementoUnoPuntoCinco.collect().forEach(System.out::println);

        // flatMap (transformación)
        JavaRDD<List<Double>> porDosyPorTresLista = numerosEnSpark.map(n -> Arrays.asList(n * 2, n * 3)).cache();
        System.out.println("Por dos y por tres lista:");
        porDosyPorTresLista.collect().forEach(System.out::println);

        JavaRDD<Double> porDosyPorTres = numerosEnSpark.flatMap(n -> Arrays.asList(n * 2, n * 3).iterator()).persist(StorageLevel.MEMORY_ONLY());
        System.out.println("Por dos y por tres:");
        porDosyPorTres.collect().forEach(System.out::println);

        // reduce (acción)
        double suma = numerosEnSpark.reduce((x, y) -> x + y);
        System.out.println("La suma es: " + suma);

        // map con reduce
        double sumaCuadrados = numerosEnSpark.map(n -> Math.pow(n, 2)).reduce((x, y) -> x + y);
        double sumaCuadrados2 = numerosEnSpark.map(n -> Math.pow(n, 2)).reduce(Double::sum);
        double sumaCuadrados3 = numerosEnSpark.mapToDouble(n -> Math.pow(n, 2)).sum();
        System.out.println("Suma cuadrados: " + sumaCuadrados);
        System.out.println("Suma cuadrados2: " + sumaCuadrados2);
        System.out.println("Suma cuadrados3: " + sumaCuadrados3);

        // fold (acción)
        double suma2 = numerosEnSpark.fold(0.0, (x, y) -> x + y);
        System.out.println("La suma es: " + suma2);

        //numerosEnSpark.reduce((x, y) -> x + ", " + y);
        //numerosEnSpark.fold("Hola", (x, y) -> x + y);

        // aggregate (acción)
        String resultado = numerosEnSpark.aggregate("A", (x, y) -> x + "C" + y, (s1, s2) -> s1 + "B" + s2);
        System.out.println("Resultado: " + resultado);

        double suma3 = numerosEnSpark.aggregate(0.0, (x, y) -> x + y, (s1, s2) -> s1 + s2);
        System.out.println("La suma es: " + suma3);

        double sumaCuadrados4 = numerosEnSpark.aggregate(0.0, (acc, n) -> acc + n * n, (s1, s2) -> s1 + s2);
        System.out.println("Suma cuadrados4: " + sumaCuadrados4);

        // zip (transformación)
        JavaRDD<String> letras = jsc.parallelize(Arrays.asList("a", "b", "c"));
        JavaRDD<Integer> enteros = jsc.parallelize(Arrays.asList(1, 2, 3));
        JavaPairRDD<String, Integer> letrasConNumeros = letras.zip(enteros);
        letrasConNumeros.collect().forEach(tupla -> {
            System.out.println("Letra con numero: " + tupla._1 + " " + tupla._2);
        });

        letras.zipWithIndex().collect().forEach(tupla -> {
            System.out.println("Letra con índice: " + tupla._1 + " " + tupla._2);
        });

        letras.zipWithUniqueId().collect().forEach(tupla -> {
            System.out.println("Letra con índice: " + tupla._1 + " " + tupla._2);
        });

        // filter (transformación)
        JavaRDD<Double> impares = numerosEnSpark.filter(n -> n % 2 == 1);
        JavaRDD<Double> pares = numerosEnSpark.filter(n -> n % 2 == 0);
        JavaRDD<Double> negativos = numerosEnSpark.filter(n -> n < 0);
        JavaRDD<Double> positivos = numerosEnSpark.filter(n -> n >= 0);

        System.out.println("impares:");
        impares.collect().forEach(System.out::println);

        System.out.println("pares:");
        pares.collect().forEach(System.out::println);

        System.out.println("negativos:");
        negativos.collect().forEach(System.out::println);

        System.out.println("positivos:");
        positivos.collect().forEach(System.out::println);

        jsc.close();
        spark.close();
    }

}
