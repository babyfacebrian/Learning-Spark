package com.BrianKalinowski.Project5;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class Application5 {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        BusinessData businessData = new BusinessData();
        Dataset<Row> data = businessData.getBusinessDataSet();

        System.out.println("Full Data");
        data.printSchema();
        data.show();

        System.out.println("Customer Stats");
        Dataset<Row> customerStats = data.groupBy("customer_id", "first_name").agg(
                count("product_name").as("number_of_purchases"),
                max("product_price").as("most_exp_purchase"),
                sum("product_price").as("total_spend"));
        customerStats.show();

        System.out.println("Product Stats");
        Dataset<Row> productStats = data.groupBy("product_name").agg(
                count("product_id").as("product_count"),
                sum("product_price").as("total_sold"))
                .orderBy("total_sold");
        productStats.show();

    }

}