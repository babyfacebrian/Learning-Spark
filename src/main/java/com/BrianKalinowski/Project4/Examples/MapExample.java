package com.BrianKalinowski.Project4.Examples;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class MapExample {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = new SparkSession.Builder().appName("Map Ex").master("local").getOrCreate();

        List<String> data = Arrays.asList("Banana", "Car", "Glass", "Banana", "Computer", "Car");

        // DATA FRAME!
        Dataset<String> dataFrame = sparkSession.createDataset(data, Encoders.STRING());

        dataFrame = dataFrame.map(new StringMapper(), Encoders.STRING());
        dataFrame.show();

        // Using lambda
        Dataset<String> dataFrameLambda = dataFrame.map((MapFunction<String, String>) r -> r + " Length: " + r.length(), Encoders.STRING());
        dataFrameLambda.show();
    }

    /*
    Serialization is a mechanism of converting the state of an object into a byte stream.
    used to persist the object.
    */
    static class StringMapper implements MapFunction<String, String>, Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public String call(String s) {
            return s + ", Length: " + s.length();
        }
    }
}
