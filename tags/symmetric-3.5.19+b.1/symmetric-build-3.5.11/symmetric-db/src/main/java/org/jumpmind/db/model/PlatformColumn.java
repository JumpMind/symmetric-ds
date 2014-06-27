package org.jumpmind.db.model;

import java.io.Serializable;

public class PlatformColumn implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;
    
    private String name;
    
    private String type;
    
    private int size = -1;
    
    private int decimalDigits = -1;
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    public int getSize() {
        return size;
    }
    
    public void setType(String type) {
        this.type = type;
    }
    
    public String getType() {
        return type;
    }
    
    public void setDecimalDigits(int decimalDigits) {
        this.decimalDigits = decimalDigits;
    }
    
    public int getDecimalDigits() {
        return decimalDigits;
    }    
        
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
