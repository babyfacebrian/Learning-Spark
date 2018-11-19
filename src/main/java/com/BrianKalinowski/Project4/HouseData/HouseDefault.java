package com.BrianKalinowski.Project4.HouseData;

import java.io.Serializable;
import java.util.Date;

public class HouseDefault implements Serializable {

    private static final long serialVersionUID = 1L;

    private int id;
    private String address;
    private int sqft;
    private double price;
    private Date vacantBy;


    public void setId(int id) {
        this.id = id;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setSqft(int sqft) {
        this.sqft = sqft;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public void setVacantBy(Date vacantBy) {
        this.vacantBy = vacantBy;
    }

    public int getId() {
        return this.id;
    }

    public String getAddress() {
        return this.address;
    }

    public int getSqft() {
        return this.sqft;
    }

    public double getPrice() {
        return this.price;
    }

    public Date getVacantBy() {
        return this.vacantBy;
    }
}
