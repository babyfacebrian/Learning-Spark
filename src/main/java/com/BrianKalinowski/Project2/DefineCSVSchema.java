package com.BrianKalinowski.Project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

public class DefineCSVSchema {

    private StructType createSchema(){
        // Create and define DataFrame struct schema and data types
        StructType schema = createStructType(new StructField[] {
                createStructField(
                        "id",
                        IntegerType,
                        false),
                createStructField(
                        "product_id",
                        IntegerType,
                        true),
                createStructField(
                        "item_name",
                        StringType,
                        false),
                createStructField(
                        "published_on",
                        DateType,
                        true),
                createStructField(
                        "url",
                        StringType,
                        false)
        });
        return schema;
    }

    private Dataset<Row> createDataFrame(SparkSession session){
        Dataset<Row> dataFrame = session.read().format("csv")
                .option("header", true)
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "M/D/Y")
                .option("quote", "^")
                .schema(createSchema())
                .load("src/main/resources/amazonProducts.txt");
        return dataFrame;
    }

    public void printDefinedSchema() {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Complex CSV with a schema to DataFrame")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = createDataFrame(sparkSession);
        df.show(5);
        df.printSchema();
    }
}
