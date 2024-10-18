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

        String env = System.getProperty("movies.env", "production");
        boolean isProduction = env.equals("production");

        SparkSession spark = SparkSession
                .builder()
                .appName(Movies.class.getSimpleName())
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setLogLevel("WARN");

        Dataset<Row> moviesDF = getMoviesDF(spark, isProduction).cache();
        traza(moviesDF, isProduction);

        Dataset<Row> ratingsDF = getRatingsDF(spark, isProduction).persist(StorageLevel.MEMORY_AND_DISK());
        traza(ratingsDF, isProduction);

        Dataset<Row> linksDF = getLinksDF(spark, isProduction).persist();
        traza(linksDF, isProduction);

        Dataset<Row> usersDF = ratingsDF.select("userId").distinct().persist();
        traza(usersDF, isProduction);

        Dataset<Row> usersSeleccionadosDF = isProduction ? usersDF : usersDF.limit(5);

        usersSeleccionadosDF.javaRDD().collect().forEach(user -> {

            Long userId = user.getLong(0);

            Dataset<Row> top10DF = calcularTop10(ratingsDF, moviesDF, linksDF, userId, isProduction);
            if (!isProduction) {
                top10DF.printSchema();
                top10DF.show(false);
            }

            if (isProduction) {
                top10DF.write().mode(SaveMode.Overwrite).parquet(getFile("paul/top10/user-" + userId, isProduction));
            } else {
                top10DF.write().mode(SaveMode.Overwrite).json(getFile("top10/user-" + userId, isProduction));
            }

        });

        jsc.close();
        spark.close();
    }

    private static void traza(Dataset<Row> df, boolean isProduction) {
        if (!isProduction) {
            df.printSchema();
            df.show();
        }
    }

    private static Dataset<Row> calcularTop10(Dataset<Row> ratingsDF, Dataset<Row> moviesDF, Dataset<Row> linksDF, Long userId, boolean isProduction) {
        Dataset<Row> ratingsDelUsuarioDF = ratingsDF.filter("userId = " + userId);
        traza(ratingsDelUsuarioDF, isProduction);

        Dataset<Row> ratingsDelUsuarioOrdenadoDF = ratingsDelUsuarioDF.sort(desc("rating"));
        traza(ratingsDelUsuarioOrdenadoDF, isProduction);

        Dataset<Row> usuarioTop10DF = ratingsDelUsuarioOrdenadoDF.limit(10);
        traza(usuarioTop10DF, isProduction);

        Dataset<Row> moviesTop10DF = usuarioTop10DF.join(moviesDF, "movieId");
        traza(moviesTop10DF, isProduction);

        Dataset<Row> linksTop10DF = moviesTop10DF.join(linksDF, "movieId");
        traza(linksTop10DF, isProduction);

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
        traza(imdbTop10DF, isProduction);

        Dataset<Row> resultadoDF = imdbTop10DF.select("rating", "title", "imdb_link", "movielens_link", "themoviedb_link");

        return resultadoDF;

    }

    private static Dataset<Row> getLinksDF(SparkSession spark, boolean isProduction) {
        StructType esquema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("imdbId", DataTypes.StringType, false),
                DataTypes.createStructField("tmdbId", DataTypes.StringType, false),
        });
        Dataset<Row> linksDF = spark
                .read()
                .option("header", "true")
                .schema(esquema)
                .csv(getFile("links.csv", isProduction));
        return linksDF;
    }

    private static Dataset<Row> getRatingsDF(SparkSession spark, boolean isProduction) {
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
                .csv(getFile("ratings.csv", isProduction));
        return ratingsDF;
    }

    private static String getFile(String name, boolean isProduction) {
        if (isProduction) {
            return "s3://saltos-school-movies/ml-latest/" + name;
        } else {
            return "src/main/resources/ml-latest-small/" + name;
        }
    }

    private static Dataset<Row> getMoviesDF(SparkSession spark, boolean isProduction) {
        StructType esquema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("title", DataTypes.StringType, false),
                DataTypes.createStructField("genres", DataTypes.StringType, false)
        });
        Dataset<Row> moviesDF = spark
                .read()
                .option("header", "true")
                .schema(esquema)
                .csv(getFile("movies.csv", isProduction));
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
