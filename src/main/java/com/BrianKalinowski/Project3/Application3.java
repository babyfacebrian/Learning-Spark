package com.BrianKalinowski.Project3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application3 {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        //create spark session
        SparkSession sparkSession = SparkSession.builder().appName("Combining Datasets").master("local").getOrCreate();

        //Durham Data
        Dataset<Row> durhamParksData = new DurhamParksData(sparkSession).getParkData();
        //durhamParksData.show(10);
        durhamParksData.printSchema();

        //Philly Data
        Dataset<Row> phillyParksData = new PhillyParksData(sparkSession).getParkData();
        //phillyParksData.show(10);
        phillyParksData.printSchema();

        //Combined data
        Dataset<Row> allParks = combineDataFrames(durhamParksData, phillyParksData);
        allParks.show(10);
        allParks.printSchema();

        //Some sql stuffs

        //parks with > 1000 acres
        allParks.filter(allParks.col("land_in_acres").gt(100.0)).groupBy(allParks.col("city")).count().show();

    }

    private static Dataset<Row> combineDataFrames(Dataset<Row> df1, Dataset<Row> df2){
        Dataset<Row> df = df1.unionByName(df2);
        return df;
    }


}
