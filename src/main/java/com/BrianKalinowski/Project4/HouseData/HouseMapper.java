package com.BrianKalinowski.Project4.HouseData;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class HouseMapper implements MapFunction<Row, HouseDefault> {
    private static final long serialVersionUID = -2L;

    @Override
    public HouseDefault call(Row row) throws ParseException {
        HouseDefault house = new HouseDefault();

        house.setId(row.getAs("id"));
        house.setAddress(row.getAs("address"));
        house.setSqft(row.getAs("sqft"));
        house.setPrice(row.getAs("price"));

        String vacancyDateString = row.getAs("vacantBy").toString();

        if (vacancyDateString != null) {
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
            house.setVacantBy(parser.parse(vacancyDateString));
        }
        return house;
    }
}
