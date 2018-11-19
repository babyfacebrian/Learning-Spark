package com.BrianKalinowski.Project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {

    void printSchema() {
        // Create spark session
        SparkSession sparkSession = SparkSession.builder().appName("CSV to Dataframe").master("local").getOrCreate();

        //load dataframe
        Dataset<Row> dataFrame = sparkSession.read().format("csv")
                .option("header", true)
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "^")
                .option("dateFormat","M/d/y")
                .option("inferSchema", true)
                .load("src/main/resources/amazonProducts.txt");

        //Print / Show partial data
        System.out.println("Excerpt of the DataFrame: ");
        dataFrame.show(7);
        //dataFrame.show(7, 90);

        // Print dataframe schema
        System.out.println("DataFrame Schema: ");
        dataFrame.printSchema();
    }
}
