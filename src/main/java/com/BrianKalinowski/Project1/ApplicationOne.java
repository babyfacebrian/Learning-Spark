package com.BrianKalinowski.Project1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;
import java.util.Scanner;
import static org.apache.spark.sql.functions.*;

public class ApplicationOne {

    public static void main(String[] args) {

        // get pw
        Scanner input = new Scanner(System.in);
        System.out.println("Enter Postgres password:");
        String pw = input.next();

        //Create spark session
        SparkSession sparkSession = new SparkSession.Builder().appName("CSV_TO_DB").master("local").getOrCreate();

        //Get Data
        Dataset<Row> dataFrame = sparkSession.read().format("csv").option("header", true)
                .load("src/main/resources/name_and_comments.txt");

        //Column transformation
        dataFrame = dataFrame.withColumn("full_name", concat(dataFrame.col("first_name"),
                lit(", "), dataFrame.col("last_name")));

        //filter with regex
        Dataset<Row> filteredDataFrame = dataFrame.filter(dataFrame.col("comment").rlike("\\d+"))
                .orderBy(dataFrame.col("last_name").asc());

        //dataFrame.show();
        //filteredDataFrame.show();

        //Database Connection and saving table
        String dbConnection = "jdbc:postgresql://localhost/postgres";
        Properties properties = new Properties();
        properties.setProperty("driver", "org.postgresql.Driver");
        properties.setProperty("user", "postgres");
        properties.setProperty("password", pw);
        filteredDataFrame.write().mode(SaveMode.Overwrite).jdbc(dbConnection, "project1", properties);

    }
}
