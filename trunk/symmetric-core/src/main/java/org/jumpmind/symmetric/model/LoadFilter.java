package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    static final Logger logger = LoggerFactory.getLogger(LoadFilter.class);

    private String loadFilterId;
    
    public enum LoadFilterType { BSH };
    
    private LoadFilterType loadFilterType = LoadFilterType.BSH;    

    private String targetCatalogName;
    
    private String targetSchemaName;
    
    private String targetTableName;
    
    private boolean filterOnUpdate = true;
    
    private boolean filterOnInsert = true;
    
    private boolean filterOnDelete = true;
    
    private String beforeWriteScript;
    
    private String afterWriteScript;
    
    private String batchCompleteScript;
    
    private String batchCommitScript;
    
    private String batchRollbackScript;
    
    private Date createTime;
    
    private String lastUpdateBy;
    
    private Date lastUpdateTime;
    
    private int loadFilterOrder;
    
    private boolean failOnError=false;

	public int getLoadFilterOrder() {
		return loadFilterOrder;
	}

	public void setLoadFilterOrder(int loadFilterOrder) {
		this.loadFilterOrder = loadFilterOrder;
	}

	public boolean isFailOnError() {
		return failOnError;
	}

	public void setFailOnError(boolean failOnError) {
		this.failOnError = failOnError;
	}

	public String getLoadFilterId() {
		return loadFilterId;
	}

	public void setLoadFilterId(String loadFilterId) {
		this.loadFilterId = loadFilterId;
	}

	public String getTargetCatalogName() {
		return targetCatalogName;
	}

	public void setTargetCatalogName(String targetCatalogName) {
		this.targetCatalogName = targetCatalogName;
	}

	public String getTargetSchemaName() {
		return targetSchemaName;
	}

	public void setTargetSchemaName(String targetSchemaName) {
		this.targetSchemaName = targetSchemaName;
	}

	public String getTargetTableName() {
		return targetTableName;
	}

	public void setTargetTableName(String targetTableName) {
		this.targetTableName = targetTableName;
	}

	public boolean isFilterOnUpdate() {
		return filterOnUpdate;
	}

	public void setFilterOnUpdate(boolean filterOnUpdate) {
		this.filterOnUpdate = filterOnUpdate;
	}

	public boolean isFilterOnInsert() {
		return filterOnInsert;
	}

	public void setFilterOnInsert(boolean filterOnInsert) {
		this.filterOnInsert = filterOnInsert;
	}

	public boolean isFilterOnDelete() {
		return filterOnDelete;
	}

	public void setFilterOnDelete(boolean filterOnDelete) {
		this.filterOnDelete = filterOnDelete;
	}

	public String getBeforeWriteScript() {
		return beforeWriteScript;
	}

	public void setBeforeWriteScript(String beforeWriteScript) {
		this.beforeWriteScript = beforeWriteScript;
	}

	public String getAfterWriteScript() {
		return afterWriteScript;
	}

	public void setAfterWriteScript(String afterWriteScript) {
		this.afterWriteScript = afterWriteScript;
	}

	public String getBatchCompleteScript() {
		return batchCompleteScript;
	}

	public void setBatchCompleteScript(String batchCompleteScript) {
		this.batchCompleteScript = batchCompleteScript;
	}

	public String getBatchCommitScript() {
		return batchCommitScript;
	}

	public void setBatchCommitScript(String batchCommitScript) {
		this.batchCommitScript = batchCommitScript;
	}

	public String getBatchRollbackScript() {
		return batchRollbackScript;
	}

	public void setBatchRollbackScript(String batchRollbackScript) {
		this.batchRollbackScript = batchRollbackScript;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}

	public String getLastUpdateBy() {
		return lastUpdateBy;
	}

	public void setLastUpdateBy(String lastUpdateBy) {
		this.lastUpdateBy = lastUpdateBy;
	}

	public Date getLastUpdateTime() {
		return lastUpdateTime;
	}

	public void setLastUpdateTime(Date lastUpdateTime) {  
		this.lastUpdateTime = lastUpdateTime;
	}

    public LoadFilterType getLoadFilterType() {
        return loadFilterType;
    }

    public void setLoadFilterType(LoadFilterType loadFilterType) {
        this.loadFilterType = loadFilterType;
    }	

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LoadFilter && loadFilterId != null) {
            return loadFilterId.equals(((LoadFilter) obj).loadFilterId);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return loadFilterId != null ? loadFilterId.hashCode() : super.hashCode();
    }
    
    @Override
    public String toString() {
        if (loadFilterId != null) {
            return loadFilterId;
        } else {
            return super.toString();
        }
    }
        
}
