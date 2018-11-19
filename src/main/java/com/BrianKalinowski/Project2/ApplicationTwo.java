package com.BrianKalinowski.Project2;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class ApplicationTwo {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        //Using spark infer schema
        InferCSVSchema parser = new InferCSVSchema();
        //parser.printSchema();

        DefineCSVSchema parser2 = new DefineCSVSchema();
        //parser2.printDefinedSchema();

        //Using Json reader
        JSONLineParser jsonLineParser = new JSONLineParser();
        jsonLineParser.parseJsonLines();
    }
}
