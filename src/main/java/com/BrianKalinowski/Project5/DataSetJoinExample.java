package com.BrianKalinowski.Project5;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataSetJoinExample {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession sparkSession = SparkSession.builder().appName("Joining stuff").master("local").getOrCreate();

        String studentFile = "/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/students.csv";
        String gradesFile = "/Users/briankalinowski/IdeaProjects/SparkProjects/src/main/resources/grade_chart.csv";

        Dataset<Row> studnetData = sparkSession.read().format("csv")
                .option("inferSchema", "true").option("header", true).load(studentFile);

        Dataset<Row> grades = sparkSession.read().format("csv")
                .option("inferSchema", "true").option("header", true).load(gradesFile);

        Dataset<Row> joinedData = studnetData.join(grades, studnetData.col("gpa")
                .equalTo(grades.col("gpa")));

        Dataset<Row> Astudents = joinedData.select(studnetData.col("student_name"),
                          studnetData.col("GPA"),
                          grades.col("letter_grade"))
                  .filter(studnetData.col("GPA").equalTo(4.0));

        Astudents.show();

        Dataset<Row> gradsCount = joinedData.groupBy("letter_grade").count();
        gradsCount.orderBy(gradsCount.col("count").desc()).show();

    }


}
