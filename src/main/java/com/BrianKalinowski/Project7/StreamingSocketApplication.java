package com.BrianKalinowski.Project7;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;s
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class StreamingSocketApplication {

    public static void main(String[] args) throws StreamingQueryException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder().appName("Streaming Word Count").master("local").getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        Dataset<Row> lines = sparkSession.readStream().format("socket").option("host", "localhost").option("port", 9999).load();

        Dataset<String> words = lines.as(Encoders.STRING()).flatMap(
                (FlatMapFunction<String, String>) w -> Arrays.asList(w.split(" ")).iterator(), Encoders.STRING());

        // Get count of words from socket
        Dataset<Row> streamWordCounts = words.groupBy("value").count();

        /*
        outputMode = update: only new words

        outputMode = complete: full data / word counts
        */
        StreamingQuery query = streamWordCounts.writeStream().outputMode("update").format("console").start();
        query.awaitTermination();

    }
}
