package com.BrianKalinowski.Project4.HouseData;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CSVToDataset {

    private static final String houseCSV = "/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/houses.csv";
    private Dataset<Row> housesDataSet;
    private Dataset<House2> houseObjectDataSet;

    CSVToDataset() {
        SparkSession sparkSession = SparkSession.builder().appName("CSV to DataSet").master("local").getOrCreate();
        createDataSet(sparkSession);
        setHouseObjectDataSet(sparkSession);
    }

    public Dataset<Row> getHousesDataSet() {
        return this.housesDataSet;
    }

    public Dataset<House2> getHouseObjectDataSet() {
        return this.houseObjectDataSet;
    }

    private void setHouseObjectDataSet(SparkSession sparkSession) {
        Dataset<Row> temp = sparkSession.read().format("csv")
                .option("inferSchema", "true")
                .option("header", true)
                .option("sep", ";")
                .load(houseCSV);
        this.houseObjectDataSet = temp.as(Encoders.bean(House2.class));
    }

    private void createDataSet(SparkSession sparkSession) {
        this.housesDataSet = sparkSession.read().format("csv")
                .option("inferSchema", "true")
                .option("header", true)
                .option("sep", ";")
                .load(houseCSV);
    }
}
