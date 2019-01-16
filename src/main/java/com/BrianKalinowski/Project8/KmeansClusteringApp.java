package com.BrianKalinowski.Project8;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class KmeansClusteringApp {

    public static void main(String[] args) {
        // Rollup messages
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // Spark Session
        SparkSession sparkSession = new SparkSession.Builder().appName("K-Means Clustering").master("local").getOrCreate();

        // Read in Data
        Dataset<Row> wholeSaleDf = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load("/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/Wholesale-customers-data.csv");
        wholeSaleDf.printSchema();
        wholeSaleDf.show();

        // Extract features
        Dataset<Row> features = wholeSaleDf.select("channel", "fresh", "milk", "grocery", "frozen", "detergents_paper", "delicassen");

        // Set Training data
        VectorAssembler assembler = new VectorAssembler();
        assembler = assembler.
                setInputCols(new String[]{"channel", "fresh", "milk", "grocery", "frozen", "detergents_paper", "delicassen"})
                .setOutputCol("features");

        Dataset<Row> trainingData = assembler.transform(features).select("features");

        // K-Means
        int numberOfClusters = 10;
        KMeans kMeans = new KMeans().setK(numberOfClusters);
        KMeansModel kMeansModel = kMeans.fit(trainingData);
        System.out.println("Model Sum of Sq errors: " + kMeansModel.computeCost(trainingData));
        kMeansModel.summary().predictions().show();
    }
}
