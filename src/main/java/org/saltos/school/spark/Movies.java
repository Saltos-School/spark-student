package org.saltos.school.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

        Dataset<Row> ratingsDF = getRatingsDF(spark);
        ratingsDF.printSchema();
        ratingsDF.show();

        Dataset<Row> linksDF = getLinksDF(spark);
        ratingsDF.printSchema();
        ratingsDF.show();

        jsc.close();
        spark.close();
    }

    private static Dataset<Row> getLinksDF(SparkSession spark) {
        StructType esquema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("movieId", DataTypes.LongType, false),
                DataTypes.createStructField("imdbId", DataTypes.LongType, false),
                DataTypes.createStructField("tmdbId", DataTypes.LongType, false),
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
