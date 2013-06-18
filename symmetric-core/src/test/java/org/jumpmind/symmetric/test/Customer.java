package org.jumpmind.symmetric.test;

import java.util.Date;

public class Customer {

    int customerId;
    String name;
    boolean active;
    String address;
    String city;
    String state;
    int zip;
    Date entryTimestamp;
    Date entryTime;
    String notes;
    byte[] icon;
    
    public Customer() {
    }

    public Customer(int customerId, String name, boolean active, String address, String city,
            String state, int zip, Date entryTimestamp, Date entryTime, String notes, byte[] icon) {
        super();
        this.customerId = customerId;
        this.name = name;
        this.active = active;
        this.address = address;
        this.city = city;
        this.state = state;
        this.zip = zip;
        this.entryTimestamp = entryTimestamp;
        this.entryTime = entryTime;
        this.notes = notes;
        this.icon = icon;
    }



    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getZip() {
        return zip;
    }

    public void setZip(int zip) {
        this.zip = zip;
    }

    public Date getEntryTimestamp() {
        return entryTimestamp;
    }

    public void setEntryTimestamp(Date entryTimestamp) {
        this.entryTimestamp = entryTimestamp;
    }

    public Date getEntryTime() {
        return entryTime;
    }

    public void setEntryTime(Date entryTime) {
        this.entryTime = entryTime;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public byte[] getIcon() {
        return icon;
    }

    public void setIcon(byte[] icon) {
        this.icon = icon;
    }

}
