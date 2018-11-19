package com.BrianKalinowski.Project3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class DurhamParksData {

    private Dataset<Row> parkData;

    DurhamParksData(SparkSession session) {
        setDataFrame(session);
    }

    public Dataset<Row> getParkData() {
        return this.parkData;
    }

    private void setDataFrame(SparkSession session) {
        this.parkData = session.read().format("json").option("multiline", true).load("src/main/resources/durham-parks.json");

        this.parkData =
                this.parkData.withColumn("park_id", concat(this.parkData.col("datasetid"), lit("_"),
                        this.parkData.col("fields.objectid"), lit("_Durham")))
                        .withColumn("park_name", this.parkData.col("fields.park_name"))
                        .withColumn("city", lit("Durham"))
                        .withColumn("address", this.parkData.col("fields.address"))
                        .withColumn("has_playground", this.parkData.col("fields.playground"))
                        .withColumn("zipcode", this.parkData.col("fields.zip"))
                        .withColumn("land_in_acres", this.parkData.col("fields.acres"))
                        .withColumn("geoX", this.parkData.col("geometry.coordinates").getItem(0))
                        .withColumn("geoY", this.parkData.col("geometry.coordinates").getItem(1))
                        .drop("fields")
                        .drop("geometry")
                        .drop("record_timestamp")
                        .drop("recordid")
                        .drop("datasetid");

    }
}
