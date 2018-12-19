package com.BrianKalinowski.Project6;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Arrays;
import static org.apache.spark.sql.functions.*;

public class RedditCommentsSmall {
    public static void main(String[] args) {
        // Error rollup
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder().appName("Reddit Comments").master("local").getOrCreate();

        /* Reddit comments downloaded at: https://files.pushshift.io/reddit/comments/ */
        String redditComments2007 = "/local file path";

        // full 2007 reddit data
        Dataset<Row> redditFullData = sparkSession.read().format("json")
                .option("inferSchema", "true")
                .option("header", true)
                .load(redditComments2007);

        // 2007 comments
        Dataset<Row> redditCommentWords = redditFullData.select("body").flatMap((FlatMapFunction<Row, String>)
                r -> Arrays.asList(r.toString().replace("\n", "").replace("\r", "")
                        .trim().toLowerCase().split(" ")).iterator(), Encoders.STRING()).toDF();

        // Filtered words
        Dataset<Row> filteredWords = sparkSession.createDataset(Arrays.asList(WordUtils.stopWords), Encoders.STRING()).toDF();

        // left join to filter out boring words and count word usage in comments
        redditCommentWords = redditCommentWords
                .join(filteredWords, redditCommentWords.col("value")
                .equalTo(filteredWords.col("value")), "leftanti")
                .groupBy("value")
                .count()
                .orderBy(desc("count"));
        redditCommentWords.show(100);
    }
}
