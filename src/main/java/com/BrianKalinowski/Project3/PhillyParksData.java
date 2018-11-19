package com.BrianKalinowski.Project3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;

import static org.apache.spark.sql.functions.*;

public class PhillyParksData {

    private Dataset<Row> parkData;

    PhillyParksData(SparkSession session) {
        setDataFrame(session);
    }

    public Dataset<Row> getParkData() {
        return this.parkData;
    }

    private void setDataFrame(SparkSession session) {
        this.parkData = session.read().format("csv").option("multiLine", true).option("header", true)
                .load("src/main/resources/philadelphia_recreations.csv");

        this.parkData.filter("lower(Use_) like '%park%'");

        this.parkData =
                this.parkData.withColumn("park_id", concat(lit("philly_"), this.parkData.col("OBJECTID")))
                        .withColumnRenamed("ASSET_NAME", "park_name")
                        .withColumn("city", lit("Philadelphia"))
                        .withColumnRenamed("ADDRESS", "address")
                        .withColumn("has_playground", lit("UNKNOWN"))
                        .withColumnRenamed("ZIPCODE", "zipcode")
                        .withColumn("ACREAGE", this.parkData.col("ACREAGE").cast(DataTypes.DoubleType))
                        .withColumnRenamed("ACREAGE", "land_in_acres")
                        .withColumn("geoX", lit("UNKNONW"))
                        .withColumn("geoY", lit("UNKNONW"))
                        .drop("SITE_NAME")
                        .drop("OBJECTID")
                        .drop("CHILD_OF")
                        .drop("TYPE")
                        .drop("USE_")
                        .drop("DESCRIPTION")
                        .drop("SQ_FEET")
                        .drop("ALLIAS")
                        .drop("CHRONOLOGY")
                        .drop("NOTES")
                        .drop("DATE_EDITED")
                        .drop("EDITED_BY")
                        .drop("OCCUPANT")
                        .drop("TENANT")
                        .drop("LABEL");
    }
}
