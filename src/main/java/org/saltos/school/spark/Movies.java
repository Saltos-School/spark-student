package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.*;

public class Movies {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName(Movies.class.getSimpleName())
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> moviesDF = getMoviesDF(spark).cache();
        moviesDF.printSchema();
        moviesDF.show();

        Dataset<Row> ratingsDF = getRatingsDF(spark).persist(StorageLevel.MEMORY_AND_DISK());
        ratingsDF.printSchema();
        ratingsDF.show();

        Dataset<Row> linksDF = getLinksDF(spark).persist();
        linksDF.printSchema();
        linksDF.show();

        Dataset<Row> usersDF = ratingsDF.select("userId").distinct().persist();
        usersDF.printSchema();
        usersDF.show();

        usersDF.limit(5).javaRDD().collect().forEach(user -> {

            Long userId = user.getLong(0);

            Dataset<Row> top10DF = calcularTop10(ratingsDF, moviesDF, linksDF, userId);
            top10DF.printSchema();
            top10DF.show(false);

            top10DF.write().mode(SaveMode.Overwrite).json("src/main/resources/top10/user-" + userId);
            top10DF.write().mode(SaveMode.Overwrite).parquet("src/main/resources/top10parquet/user-" + userId);
            top10DF.write().mode(SaveMode.Overwrite).orc("src/main/resources/top10orc/user-" + userId);

        });

        jsc.close();
        spark.close();
    }

    private static Dataset<Row> calcularTop10(Dataset<Row> ratingsDF, Dataset<Row> moviesDF, Dataset<Row> linksDF, Long userId) {
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

        Dataset<Row> imdbTop10DF = linksTop10DF.withColumn("imdb_link",
                concat(
                    lit("https://www.imdb.com/title/tt"),
                    col("imdbid")
                )
        ).withColumn("movielens_link",
                concat(
                        lit("https://movielens.org/movies/"),
                        col("movieId")
                )
        ).withColumn("themoviedb_link",
                concat(
                        lit("https://www.themoviedb.org/movie/"),
                        col("tmdbid")
                )
        );
        imdbTop10DF.printSchema();
        imdbTop10DF.show();

        Dataset<Row> resultadoDF = imdbTop10DF.select("rating", "title", "imdb_link", "movielens_link", "themoviedb_link");

        return resultadoDF;

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
