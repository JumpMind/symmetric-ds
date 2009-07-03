/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.jumpmind.symmetric.service.IConfigurationService;

/**
 * This is the data that changed due to a data sync trigger firing.
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
     * To support column-level sync and conflict resolution.
     * Comma delimited old row data from an update.
     */

    private String oldData;

    /**
     * This is a reference to the triggerHistory row the trigger refered to when the data
     * event fired.
     */
    private TriggerHistory triggerHistory;    

    private DataEventType eventType;

    private String tableName;

    /**
     * This is populated by the trigger when the event happens. It will be
     * useful for research.
     */
    private Date createTime;

    public Data(long dataId, String pkData, String rowData, DataEventType eventType, String tableName, Date createTime,
            TriggerHistory triggerHistory) {
        super();
        this.dataId = dataId;
        this.pkData = pkData;
        this.rowData = rowData;
        this.eventType = eventType;
        this.tableName = tableName;
        this.createTime = createTime;
        this.triggerHistory = triggerHistory;
    }

    public Data(String tableName, DataEventType eventType, String rowData, String pkData, TriggerHistory triggerHistory) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowData = rowData;
        this.pkData = pkData;
        this.triggerHistory = triggerHistory;
    }
    
    public Data(ResultSet results, IConfigurationService configurationService)
            throws SQLException {
        this.dataId = results.getLong(1);
        this.tableName = results.getString(2);
        this.eventType = DataEventType.getEventType(results.getString(3));
        this.rowData = results.getString(4);
        this.pkData = results.getString(5);
        this.oldData = results.getString(6);
        this.createTime = results.getDate(7);
        int histId = results.getInt(8);
        this.triggerHistory = configurationService.getHistoryRecordFor(histId);
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

    public TriggerHistory getTriggerHistory() {
        return triggerHistory;
    }

    public void setTriggerHistory(TriggerHistory triggerHistory) {
        this.triggerHistory = triggerHistory;
    }

    public String getOldData() {
        return oldData;
    }

    public void setOldData(String oldData) {
        this.oldData = oldData;
    }

}
