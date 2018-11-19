package com.BrianKalinowski.Project4.HouseData;

import java.io.Serializable;
import java.util.Date;

public class House2 implements Serializable {

    private static final long serialVersionUID = 1L;

    private int id;
    private String address;
    private int sqft;
    private double price;
    private Date vacantBy;


    public House2(int id, String address, int sqft, double price, Date vacantBy) {
        this.id = id;
        this.address = address;
        this.sqft = sqft;
        this.price = price;
        this.vacantBy = vacantBy;
    }

    public int getId() {
        return id;
    }

    public String getAddress() {
        return address;
    }

    public int getSqft() {
        return sqft;
    }

    public double getPrice() {
        return price;
    }

    public Date getVacantBy() {
        return vacantBy;
    }
}
