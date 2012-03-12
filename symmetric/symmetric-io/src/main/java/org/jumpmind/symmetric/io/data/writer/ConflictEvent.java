package org.jumpmind.symmetric.io.data.writer;

import java.io.Serializable;
import java.util.Date;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;

public class ConflictEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        OK, CD, IG
    };
    
    public enum ConflictType {
        PK, FK
    }

    private long batchId;
    private String nodeId;
    private int failedRowNumber;
    private String conflictId;
    private Status status;
    private ConflictType conflictType;
    private String targetCatalogName;
    private String targetSchemaName;
    private String targetTableName;
    private DataEventType eventType;
    private String message;
    private String rowData;
    private String oldData;
    private String finalData;
    private int retryCount;
    private Date createTime;
    private String lastUpdateBy;
    private Date lastUpdateTime;
    
    public ConflictEvent(Batch batch, ConflictSetting conflictSettings, Table table, Status status, CsvData data, String message) {
        this.batchId = batch.getBatchId();
        this.conflictId = conflictSettings.getConflictSettingId();
        this.targetCatalogName = table.getCatalog();
        this.targetSchemaName = table.getSchema();
        this.targetTableName = table.getName();
        this.eventType = data.getDataEventType();
        this.rowData = data.getCsvData(CsvData.ROW_DATA);
        this.oldData = data.getCsvData(CsvData.OLD_DATA);
        this.status = status;
        this.message = message;
        this.lastUpdateBy = "symmetricds";       
    }
        
    public ConflictEvent() {     
    }
    
    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getFailedRowNumber() {
        return failedRowNumber;
    }

    public void setFailedRowNumber(int failedRowNumber) {
        this.failedRowNumber = failedRowNumber;
    }

    public String getConflictId() {
        return conflictId;
    }

    public void setConflictId(String conflictId) {
        this.conflictId = conflictId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
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

    public DataEventType getEventType() {
        return eventType;
    }

    public void setEventType(DataEventType eventType) {
        this.eventType = eventType;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String errorMessage) {
        this.message = errorMessage;
    }

    public String getRowData() {
        return rowData;
    }

    public void setRowData(String rowData) {
        this.rowData = rowData;
    }

    public String getOldData() {
        return oldData;
    }

    public void setOldData(String oldData) {
        this.oldData = oldData;
    }

    public String getFinalData() {
        return finalData;
    }

    public void setFinalData(String finalData) {
        this.finalData = finalData;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
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

    public void setConflictType(ConflictType conflictType) {
        this.conflictType = conflictType;
    }
    
    public ConflictType getConflictType() {
        return conflictType;
    }
    
}
