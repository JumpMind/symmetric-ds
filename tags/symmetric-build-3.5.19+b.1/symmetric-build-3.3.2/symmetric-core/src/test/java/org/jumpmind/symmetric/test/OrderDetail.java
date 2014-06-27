package org.jumpmind.symmetric.test;

import java.math.BigDecimal;

public class OrderDetail {
    
    private String orderId;
    private int lineNumber;
    private String itemType;
    private String itemId;
    private int quantity;
    private BigDecimal price;
    
    public OrderDetail(String orderId, int lineNumber, String itemType, String itemId,
            int quantity, BigDecimal price) {
        this.orderId = orderId;
        this.lineNumber = lineNumber;
        this.itemType = itemType;
        this.itemId = itemId;
        this.quantity = quantity;
        this.price = price;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(int lineNumber) {
        this.lineNumber = lineNumber;
    }

    public String getItemType() {
        return itemType;
    }

    public void setItemType(String itemType) {
        this.itemType = itemType;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    
    
}
