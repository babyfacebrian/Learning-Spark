package com.BrianKalinowski.Project7;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

public class StreamingKafkaConsumer {

    public static void main(String[] args) throws StreamingQueryException {

        SparkSession sparkSession = SparkSession.builder().appName("Kafka Streaming").master("local").getOrCreate();

        // Kafka Consumer
        Dataset<Row> messageData = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "bk_test").load()
                .selectExpr("CAST(value AS STRING)");

        Dataset<String> messages = messageData.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCounts = messages.groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream().outputMode("complete").format("console").start();

        query.awaitTermination();
    }
}
