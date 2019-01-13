package com.BrianKalinowski.Project8;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LogisticRegressionApp {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = new SparkSession.Builder().appName("Logistic Regression Example").master("local").getOrCreate();

        // Read in cryo treatment data
        Dataset<Row> cryoTreatmetData = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .format("csv")
                .load("/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/cryotherapy.csv");

        // Column re-naming / formatting
        Dataset<Row> treatmentDf = cryoTreatmetData.withColumnRenamed("Result_of_Treatment", "label")
                .select("label", "sex", "age", "time", "number_of_warts", "type", "area")
                .na().drop();
        treatmentDf.printSchema();

        StringIndexer genderIndexer = new StringIndexer().setInputCol("sex").setOutputCol("sexIndex");

        // Vectorize features
        VectorAssembler assembler = new VectorAssembler()
                .setInputCols(new String[]{"sexIndex", "age", "time", "number_of_warts", "type", "area"})
                .setOutputCol("features");

        // Split training / testing data
        Dataset<Row>[] splitData = treatmentDf.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingDf = splitData[0];
        Dataset<Row> testingDf = splitData[1];

        // Logistic Regression
        LogisticRegression logisticRegression = new LogisticRegression();
        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{genderIndexer, assembler, logisticRegression});

        // Model Fitting
        PipelineModel model = pipeline.fit(trainingDf);
        Dataset<Row> results = model.transform(testingDf);
        results.show();
    }
}