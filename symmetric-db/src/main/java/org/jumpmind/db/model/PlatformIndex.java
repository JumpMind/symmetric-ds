package org.jumpmind.db.model;

import java.io.Serializable;

public class PlatformIndex implements Serializable, Cloneable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private String name;
    
    private String filterCondition;
    
    public PlatformIndex() {
        
    }
    
    public PlatformIndex(String indexName, String filter) {
        this.name=indexName;
        this.filterCondition = filter;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

   @Override
    public PlatformIndex clone() throws CloneNotSupportedException {
        PlatformIndex platformIndex = new PlatformIndex(this.name, this.filterCondition);
        return platformIndex;
    }
}
