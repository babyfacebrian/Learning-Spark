package com.BrianKalinowski.Project4.WordCounts;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WordCount {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        ShakeSpearData data = new ShakeSpearData();
        Dataset<Row> dataSet = data.getWordValueCounts();
        dataSet.show(50);


    }
}
