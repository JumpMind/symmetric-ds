package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;

/**
 * This is the data that changed due to a data sync trigger firing.
 */
public class Data extends CsvData implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * This is a reference to the triggerHistory row the trigger referred to
     * when the data event fired.
     */
    private TriggerHistory triggerHistory;

    public Data(long dataId, String pkData, String rowData, DataEventType eventType,
            String tableName, Date createTime, TriggerHistory triggerHistory, String channelId,
            String transactionId, String sourceNodeId) {
        super();
        this.setDataId(dataId);
        this.setPkData(pkData);
        this.setRowData(rowData);
        this.setDataEventType(eventType);
        this.setTableName(tableName);
        this.setCreateTime(createTime);
        this.setChannelId(channelId);
        this.setTransactionId(transactionId);
        this.setSourceNodeId(sourceNodeId);
        this.triggerHistory = triggerHistory;
    }

    public Data(String tableName, DataEventType eventType, String rowData, String pkData,
            TriggerHistory triggerHistory, String channelId, String transactionId,
            String sourceNodeId) {
        this(-1, pkData, rowData, eventType, tableName, new Date(), triggerHistory, channelId,
                transactionId, sourceNodeId);
    }

    public Data() {
    }

    public String[] toParsedRowData() {
        return getParsedData(ROW_DATA);
    }

    public String[] toParsedOldData() {
        return getParsedData(OLD_DATA);
    }

    public String[] toParsedPkData() {
        return getParsedData(PK_DATA);
    }

    public long getDataId() {
        Long dataId = getAttribute(ATTRIBUTE_DATA_ID);
        if (dataId != null) {
            return dataId;
        } else {
            return -1l;
        }
    }

    public void setDataId(long dataId) {
        putAttribute(ATTRIBUTE_DATA_ID, dataId);
    }

    public String getTableName() {
        return getAttribute(ATTRIBUTE_TABLE_NAME);
    }

    public void setTableName(String tableName) {
        putAttribute(ATTRIBUTE_TABLE_NAME, tableName);
    }
    
    public String getRowData() {
        return getCsvData(ROW_DATA);
    }

    public void setRowData(String rowData) {
        putCsvData(ROW_DATA, rowData);
    }

    public String getPkData() {
        return getCsvData(PK_DATA);
    }

    public void setPkData(String pkData) {
        putCsvData(PK_DATA, pkData);
    }

    public String getOldData() {
        return getCsvData(OLD_DATA);
    }

    public void setOldData(String oldData) {
        putCsvData(OLD_DATA, oldData);
    }

    public TriggerHistory getTriggerHistory() {
        return triggerHistory;
    }

    public void setTriggerHistory(TriggerHistory triggerHistory) {
        this.triggerHistory = triggerHistory;
    }

    public String getChannelId() {
        return getAttribute(ATTRIBUTE_CHANNEL_ID);
    }

    public void setChannelId(String channelId) {
        putAttribute(ATTRIBUTE_CHANNEL_ID, channelId);
    }

    public String getTransactionId() {
        return getAttribute(ATTRIBUTE_TX_ID);
    }

    public void setTransactionId(String transactionId) {
        putAttribute(ATTRIBUTE_TX_ID, transactionId);
    }

    public String getSourceNodeId() {
        return getAttribute(ATTRIBUTE_SOURCE_NODE_ID);
    }

    public void setSourceNodeId(String sourceNodeId) {
        putAttribute(ATTRIBUTE_SOURCE_NODE_ID, sourceNodeId);
    }

    public String getExternalData() {
        return getAttribute(ATTRIBUTE_EXTERNAL_DATA);
    }

    public void setExternalData(String externalData) {
        putAttribute(ATTRIBUTE_EXTERNAL_DATA, externalData);
    }

    public Date getCreateTime() {
        return getAttribute(ATTRIBUTE_CREATE_TIME);
    }

    public void setCreateTime(Date createTime) {
        putAttribute(ATTRIBUTE_CREATE_TIME, createTime);
    }

    public String getPkDataFor(String columnName) {
        String[] pkData = toParsedPkData();
        String[] keyNames = triggerHistory.getParsedPkColumnNames();
        for (int i = 0; i < keyNames.length; i++) {
            if (columnName.equals(keyNames[i])) {
                return pkData[i];
            }
        }
        return null;
    }

}