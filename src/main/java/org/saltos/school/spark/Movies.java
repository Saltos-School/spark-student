package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

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

        Dataset<Row> ratingsDF = getRatingsDF(spark).persist();
        ratingsDF.printSchema();
        ratingsDF.show();

        Dataset<Row> linksDF = getLinksDF(spark);
        linksDF.printSchema();
        linksDF.show();

        Dataset<Row> usersDF = ratingsDF.select("userId").distinct();
        usersDF.printSchema();
        usersDF.show();

        Long userId = 5L;

        calcularTop10(ratingsDF, moviesDF, linksDF, userId);

        jsc.close();
        spark.close();
    }

    private static void calcularTop10(Dataset<Row> ratingsDF, Dataset<Row> moviesDF, Dataset<Row> linksDF, Long userId) {
        Dataset<Row> ratingsDelUsuarioDF = ratingsDF.filter("userId = " + userId);
        ratingsDelUsuarioDF.printSchema();
        ratingsDelUsuarioDF.show();

        Dataset<Row> ratingsDelUsuarioOrdenadoDF = ratingsDelUsuarioDF.sort(desc("rating"));
        ratingsDelUsuarioOrdenadoDF.printSchema();
        ratingsDelUsuarioOrdenadoDF.show();

        Dataset<Row> usuarioTop10DF = ratingsDelUsuarioOrdenadoDF.limit(10);
        usuarioTop10DF.printSchema();
        usuarioTop10DF.show();

        Dataset<Row> moviesTop10DF = usuarioTop10DF.join(moviesDF, "movieId");
        moviesTop10DF.printSchema();
        moviesTop10DF.show();

        Dataset<Row> linksTop10DF = moviesTop10DF.join(linksDF, "movieId");
        linksTop10DF.printSchema();
        linksTop10DF.show();

    }

    private static Dataset<Row> getLinksDF(SparkSession spark) {
        StructType esquema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("imdbId", DataTypes.StringType, false),
                DataTypes.createStructField("tmdbId", DataTypes.StringType, false),
        });
        Dataset<Row> linksDF = spark
                .read()
                .option("header", "true")
                .schema(esquema)
                .csv("src/main/resources/ml-latest-small/links.csv");
        return linksDF;
    }

    private static Dataset<Row> getRatingsDF(SparkSession spark) {
        StructType esquema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("userId", DataTypes.LongType, false),
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("rating", DataTypes.DoubleType, false),
                DataTypes.createStructField("timestamp", DataTypes.LongType, false)
        });
        Dataset<Row> ratingsDF = spark
                .read()
                .option("header", "true")
                .schema(esquema)
                .csv("src/main/resources/ml-latest-small/ratings.csv");
        return ratingsDF;
    }

    private static Dataset<Row> getMoviesDF(SparkSession spark) {
        StructType esquema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("genres", DataTypes.StringType, false)
        });
        Dataset<Row> moviesDF = spark
                .read()
                .option("header", "true")
                .schema(esquema)
                .csv("src/main/resources/ml-latest-small/movies.csv");
        StructType esquema2 = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("genres", DataTypes.createArrayType(DataTypes.StringType), false)
        });
        Encoder<Row> encoder2 = RowEncoder.apply(esquema2);
        Dataset<Row> moviesDF2 = moviesDF.map((MapFunction<Row, Row>)  fila -> {
            Long movieId = fila.getLong(0);
            String title = fila.getString(1);
            String genres = fila.getString(2);
            String[] genres2 = genres.split("\\|");
            Row fila2 = RowFactory.create(movieId, title, genres2);
            return fila2;
        }, encoder2);
        return moviesDF2;
    }

}
