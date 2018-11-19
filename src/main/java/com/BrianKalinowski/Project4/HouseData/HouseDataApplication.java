package com.BrianKalinowski.Project4.HouseData;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;


public class HouseDataApplication {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        CSVToDataset houseData = new CSVToDataset();
        Dataset<Row> housesDataSet = houseData.getHousesDataSet();

        // With HouseMapper
        Dataset<HouseDefault> houseDS = housesDataSet.map(new HouseMapper(), Encoders.bean(HouseDefault.class));
        houseDS.printSchema();
        houseDS.show();

        // Without houseMapper
        Dataset<House2> houseObjectData = houseData.getHouseObjectDataSet();
        houseObjectData.printSchema();
        houseObjectData.show();
    }
}
