package org.jumpmind.symmetric.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Order {

    private String orderId;
    private int customerId;
    private String status;
    private Date deliverDate;
    
    private List<OrderDetail> orderDetails = new ArrayList<OrderDetail>();

    public Order(String orderId, int customerId, String status, Date deliverDate) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.status = status;
        this.deliverDate = deliverDate;
    }
    
    public List<OrderDetail> getOrderDetails() {
        return orderDetails;
    }
    
    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getDeliverDate() {
        return deliverDate;
    }

    public void setDeliverDate(Date deliverDate) {
        this.deliverDate = deliverDate;
    }

}
