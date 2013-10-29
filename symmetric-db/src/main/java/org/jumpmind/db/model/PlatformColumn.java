package org.jumpmind.db.model;

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.Serializable;

public class PlatformColumn implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;
    
    private String name;
    
    private String type;
    
    private String size;
    
    private int decimalDigits;
    
    public void setName(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public void setSize(String size) {
        this.size = size;
    }
    
    public String getSize() {
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
    
    public String toSizeSpec() {
        String sizeSpec = null;
        if (isNotBlank(size)) {
            sizeSpec = size;
            if (decimalDigits > -1) {
                sizeSpec = sizeSpec + "," + decimalDigits;
            }
        }
        return sizeSpec;
    }
        
    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
