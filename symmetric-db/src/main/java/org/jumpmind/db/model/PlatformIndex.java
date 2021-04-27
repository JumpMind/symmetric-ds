package org.jumpmind.db.model;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;

public class PlatformIndex implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;
    
    private String name;
    
    private String filterCondition;
    
    private CompressionTypes compressionType;

    public PlatformIndex() {
        this(null, null, CompressionTypes.NONE);
    }
    
    public PlatformIndex(String indexName, String filter) {
        this(indexName, filter, CompressionTypes.NONE);
    }
    
    public PlatformIndex(String indexName, String filter, CompressionTypes compressionType) {
        this.name=indexName;
        this.filterCondition = filter;
        this.compressionType = compressionType;
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
    
    public CompressionTypes getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionTypes compressionType) {
        this.compressionType = compressionType;
    }
    
    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((compressionType == null) ? 0 : compressionType.hashCode());
		result = prime * result + ((filterCondition == null) ? 0 : filterCondition.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

    @Override
    public boolean equals(Object index) {
        boolean ret = false;
        if(index instanceof PlatformIndex) {
            PlatformIndex platformIndex = (PlatformIndex) index;
            ret = new EqualsBuilder().append(this.name, platformIndex.name)
                    .append(this.filterCondition, platformIndex.filterCondition)
                    .append(this.compressionType, platformIndex.compressionType)
                    .isEquals();
        }
        return ret;
    }

   @Override
    public PlatformIndex clone() throws CloneNotSupportedException {
        PlatformIndex platformIndex = new PlatformIndex(this.name, this.filterCondition, this.compressionType);
        return platformIndex;
    }
}
