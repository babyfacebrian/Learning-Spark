package com.BrianKalinowski.Project7;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

public class StreamingFileDirectoryApp {

    public static void main(String[] args) throws StreamingQueryException {

        //Spark Session
        SparkSession sparkSession = SparkSession.builder().appName("Streaming File Directory").master("local").getOrCreate();

        // Data Set Schema
        StructType userSchema = new StructType().add("date", "string").add("value", "float");

        // Listing for files added to the given path
        Dataset<Row> stockData = sparkSession.readStream().option("sep", ",").schema(userSchema)
                .csv("/Users/briankalinowski/Desktop/STOCKMARKET_csv_2/IncomingStockFiles");

        // Avg. Stock prices grouped by date
        Dataset<Row> resultStockData = stockData.groupBy("date").agg(avg(stockData.col("value")));

        StreamingQuery query = resultStockData.writeStream().outputMode("complete").format("console").start();
        query.awaitTermination();

    }
}
