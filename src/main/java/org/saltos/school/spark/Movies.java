package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Movies {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(Movies.class.getSimpleName())
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> moviesDF = getMoviesDF(spark);
        moviesDF.printSchema();
        moviesDF.show();

        jsc.close();
        spark.close();
    }

    private static Dataset<Row> getMoviesDF(SparkSession spark) {
        return spark
                .read()
                .option("header", "true")
                .csv("src/main/resources/ml-latest-small/movies.csv");
    }

}
