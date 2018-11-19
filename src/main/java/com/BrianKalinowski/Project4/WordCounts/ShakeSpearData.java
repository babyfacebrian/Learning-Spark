package com.BrianKalinowski.Project4.WordCounts;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.List;
import java.util.Iterator;

public class ShakeSpearData {

    protected List<String> commonWords;

//    private final String commonWords = " ('a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by',\r\n" +
//            "'for', 'if', 'in', 'into', 'is', 'it',\r\n" +
//            "'no', 'not', 'of', 'on', 'or', 'such',\r\n" +
//            "'that', 'the', 'their', 'then', 'there', 'these',\r\n" +
//            "'they', 'this', 'to', 'was', 'will', 'with', 'he', 'she'," +
//            "'your', 'you', 'I', "
//            + " 'i','[',']', '[]', 'his', 'him', 'our', 'we') ";


    private final String shakespeareFile = "/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/shakespeare.txt";
    private Dataset<Row> wordsDataSet;
    private Dataset<String> wordValues;

    ShakeSpearData() {
        SparkSession sparkSession = SparkSession.builder().appName("Text to FlatMap").master("local").getOrCreate();
        setCommonWords();
        setWordsDataSet(sparkSession);
        setWordValues();
    }


    public Dataset<Row> getWordValueCounts() {
        Dataset<Row> counts = this.wordValues.toDF("word").groupBy("word").count();
        counts = counts.orderBy(counts.col("count").desc());
        return counts.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) {
                List<String> common = Arrays.asList(" ", "a", "and", "are", "as", "at", "am", "all", "be", "but", "by",
                        "for", "if", "in", "into", "is", "it", "I'll", "my", "so", "do", "her", "him", "than", "that",
                        "the", "there", "their", "these", "they", "like", "to", "was", "will", "with", "like", "would",
                        "make", "I", "you", "your", "may", "O", "you,", "did", "them", "when", "some", "My", "were", "when",
                        "from", "such", "no", "yes", "us", "which", "know", "how", "[]", "[,]", "our", "we");
                return !common.contains(row.toString());
            }
        });
    }

    private void setCommonWords() {
        this.commonWords = Arrays.asList(" ", "a", "and", "are", "as", "at", "am", "all", "be", "but", "by",
                "for", "if", "in", "into", "is", "it", "I'll", "my", "so", "do", "her", "him", "than", "that",
                "the", "there", "their", "these", "they", "like", "to", "was", "will", "with", "like", "would",
                "make", "I", "you", "your", "may", "O", "you,", "did", "them", "when", "some", "My", "were", "when",
                "from", "such", "no", "yes", "us", "which", "know", "how", "[]", "[,]", "our", "we");
    }

    private void setWordValues() {
        this.wordValues = this.wordsDataSet.flatMap(new LineMapper(), Encoders.STRING());
    }

    private void setWordsDataSet(SparkSession sparkSession) {
        this.wordsDataSet = sparkSession.read().format("text").load(this.shakespeareFile);
    }

    static class LineMapper implements FlatMapFunction<Row, String> {

        @Override
        public Iterator<String> call(Row row) throws Exception {
            return Arrays.asList(row.toString().split(" ")).iterator();
        }
    }


}
