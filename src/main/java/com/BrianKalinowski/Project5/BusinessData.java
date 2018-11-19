package com.BrianKalinowski.Project5;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class BusinessData {

    private Dataset<Row> businessDataSet;

    BusinessData() {
        SparkSession sparkSession = SparkSession.builder().appName("Business Data").master("local").getOrCreate();
        joinBusinessData(sparkSession);
    }

    Dataset<Row> getBusinessDataSet() {
        return this.businessDataSet;
    }

    private void joinBusinessData(SparkSession sparkSession) {
        String customersFile = "/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/customers.csv";
        Dataset<Row> customerData = sparkSession.read().format("csv")
                .option("inferSchema", "true").option("header", true).load(customersFile);

        String productsFile = "/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/products.csv";
        Dataset<Row> productData = sparkSession.read().format("csv")
                .option("inferSchema", "true").option("header", true).load(productsFile);

        String purchasesFile = "/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/purchases.csv";
        Dataset<Row> purchaseData = sparkSession.read().format("csv")
                .option("inferSchema", "true").option("header", true).load(purchasesFile);

        this.businessDataSet = customerData.join(purchaseData, customerData.col("customer_id")
                .equalTo(purchaseData.col("customer_id"))).join(productData, purchaseData.col("product_id")
                .equalTo(productData.col("product_id")))
                .drop(purchaseData.col("customer_id"))
                .drop(purchaseData.col("product_id"));
    }

}
