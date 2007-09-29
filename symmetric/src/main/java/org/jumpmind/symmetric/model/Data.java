package org.jumpmind.symmetric.model;

import java.util.Date;

/**
 * This is the data that changed due to a data sync trigger firing.
 * @author chenson
 */
public class Data {

    // PK
    private long dataId;

    /**
     * Comma deliminated primary key data.
     */
    private String pkData;

    /**
     * Comma deliminated row data.
     */
    private String rowData;

    /**
     * This is a reference to the audit row the trigger refered to when the data event fired.
     */
    private TriggerHistory audit;

    private DataEventType eventType;

    private String tableName;

    /**
     * This is populated by the trigger when the event happens.  It will be useful for 
     * research.
     */
    private Date createTime;

    /**
     * This is populated by the trigger if a batchId select
     */
    private String batchId;

    public Data(long dataId, String pkData, String rowData,
            DataEventType eventType, String tableName, String batchId,
            Date createTime, TriggerHistory audit) {
        super();
        this.dataId = dataId;
        this.pkData = pkData;
        this.rowData = rowData;
        this.eventType = eventType;
        this.tableName = tableName;
        this.batchId = batchId;
        this.createTime = createTime;
        this.audit = audit;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public long getDataId() {
        return dataId;
    }

    public void setDataId(long dataId) {
        this.dataId = dataId;
    }

    public DataEventType getEventType() {
        return eventType;
    }

    public void setEventType(DataEventType eventType) {
        this.eventType = eventType;
    }

    public String getPkData() {
        return pkData;
    }

    public void setPkData(String pkData) {
        this.pkData = pkData;
    }

    public String getRowData() {
        return rowData;
    }

    public void setRowData(String rowData) {
        this.rowData = rowData;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public TriggerHistory getAudit()
    {
        return audit;
    }

    public void setAudit(TriggerHistory audit)
    {
        this.audit = audit;
    }

}
