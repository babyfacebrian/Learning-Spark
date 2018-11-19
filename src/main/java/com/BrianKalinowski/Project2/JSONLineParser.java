package com.BrianKalinowski.Project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLineParser {

    void parseJsonLines() {
        SparkSession sparkSession = SparkSession.builder().appName("JSON to DataFrame").master("local").getOrCreate();

        //Single line Json
        Dataset<Row> df1 = sparkSession.read().format("json").load("src/main/resources/simple.json");

        //Multi-line json
        Dataset<Row> df2 = sparkSession.read().format("json").option("multiline", true)
                .load("src/main/resources/multiline.json");

        System.out.println("Simple Json Data");
        df1.show();
        df1.printSchema();

        System.out.println("Multi-Line Json Data");
        df2.show(7,90);
        df2.printSchema();

    }
}
