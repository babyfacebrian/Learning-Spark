package com.BrianKalinowski.Project4.Examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

import java.util.*;

public class ArrayToDataSet {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = new SparkSession.Builder().appName("Array to DataSet").master("local").getOrCreate();

        List<String> data = Arrays.asList("Banana", "Car", "Glass", "Banana", "Computer", "Car");

        // DATA FRAME!
        Dataset<String> dataFrame = sparkSession.createDataset(data, Encoders.STRING());

        // DATA SET! (spark row)
        Dataset<Row> dataset = dataFrame.toDF();
        dataset.printSchema();
        dataset.show();
        dataset.groupBy("value").count().show();

    }
}
