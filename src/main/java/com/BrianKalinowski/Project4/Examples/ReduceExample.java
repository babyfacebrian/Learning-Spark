package com.BrianKalinowski.Project4.Examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class ReduceExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = new SparkSession.Builder().appName("Reduce Ex").master("local").getOrCreate();

        List<String> data = Arrays.asList("Banana", "Car", "Glass", "Banana", "Computer", "Car");

        // DATA FRAME!
        Dataset<String> dataFrame = sparkSession.createDataset(data, Encoders.STRING());

        // Combines to all values to one string
        String stringValue = dataFrame.reduce(new StringReducer());
        System.out.println("String Reduced: " + stringValue);
    }

    /*
    Serialization is a mechanism of converting the state of an object into a byte stream.
    used to persist the object.
    */
    static class StringReducer implements ReduceFunction<String>, Serializable {

        @Override
        public String call(String s, String t1) {
            return s + ", " + t1;
        }
    }
}
