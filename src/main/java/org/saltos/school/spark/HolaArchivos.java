package org.saltos.school.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

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

        Dataset<Row> peopleTXT = spark
            .read()
            .format("csv")
            .option("sep", ",")
            .load("src/main/resources/people.txt");
        peopleTXT.printSchema();
        peopleTXT.show();

        Dataset<Row> peopleTXT2 = spark.read().text("src/main/resources/people.txt");
        peopleTXT2.printSchema();
        peopleTXT2.show();

        JavaRDD<String> peopleRDD = jsc.textFile("src/main/resources/people.txt");
        peopleRDD.collect().forEach(System.out::println);

        JavaRDD<Row> peopleRow = peopleRDD.map(linea -> {
            String[] partes = linea.split(",");
            String nombre = partes[0].trim();
            Integer edad = Integer.parseInt(partes[1].trim());
            Row fila = RowFactory.create(nombre, edad);
            return fila;
        });
        peopleRow.collect().forEach(System.out::println);

        StructField nombreField = DataTypes.createStructField("nombre", DataTypes.StringType, false);
        StructField edadField = DataTypes.createStructField("edad", DataTypes.IntegerType, true);
        StructType esquema = DataTypes.createStructType(Arrays.asList(nombreField, edadField));

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRow, esquema);
        peopleDF.printSchema();
        peopleDF.show();

        Dataset<Row> peopleTXT3 = spark.createDataFrame(peopleTXT.javaRDD(), esquema);
        peopleTXT3.printSchema();
        peopleTXT3.show();

        Dataset<Row> peopleJson = spark.read().json("src/main/resources/people.json");
        Dataset<Row> peopleJson2 = spark.read().format("json").load("src/main/resources/people.json");

        JavaRDD<Row> peopleJsonRDD = peopleJson2.javaRDD();

        peopleJson.printSchema();
        peopleJson.show();

        jsc.close();
        spark.close();
    }

}
