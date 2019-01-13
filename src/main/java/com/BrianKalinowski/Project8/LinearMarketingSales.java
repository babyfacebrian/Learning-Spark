package com.BrianKalinowski.Project8;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LinearMarketingSales {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = new SparkSession.Builder().appName("Linear Regression Example").master("local").getOrCreate();

        // Read in sales data
        Dataset<Row> salesData = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load("/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/marketing-vs-sales.csv");

        // Re-format columns
        Dataset<Row> salesML = salesData
                .withColumnRenamed("sales", "label")
                .select("label", "marketing", "staff_bit");

        // Feature columns to vector
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"marketing", "staff_bit"})
                .setOutputCol("features");

        // Vector Transform DataSet
        Dataset<Row> featuresDF = assembler.transform(salesML).select("label", "features");
        featuresDF = featuresDF.na().drop();
        featuresDF.printSchema();

        // Linear Regression
        LinearRegression linearRegression = new LinearRegression();
        LinearRegressionModel model = linearRegression.fit(featuresDF);

        // Stats Summary Values
        System.out.println("R2: " + model.summary().r2());
        System.out.println("R2 Adjusted: " + model.summary().r2adj());
        System.out.println("Mean Sq Error: " + model.summary().meanSquaredError());

        // LR Predictions
        model.summary().predictions().show(100);
    }
}
