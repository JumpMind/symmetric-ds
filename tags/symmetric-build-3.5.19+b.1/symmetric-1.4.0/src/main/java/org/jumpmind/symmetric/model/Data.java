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

import java.util.Date;

/**
 * This is the data that changed due to a data sync trigger firing.
 * 
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
     * This is a reference to the audit row the trigger refered to when the data
     * event fired.
     */
    private TriggerHistory audit;

    private DataEventType eventType;

    private String tableName;

    /**
     * This is populated by the trigger when the event happens. It will be
     * useful for research.
     */
    private Date createTime;

    public Data(long dataId, String pkData, String rowData, DataEventType eventType, String tableName, Date createTime,
            TriggerHistory audit) {
        super();
        this.dataId = dataId;
        this.pkData = pkData;
        this.rowData = rowData;
        this.eventType = eventType;
        this.tableName = tableName;
        this.createTime = createTime;
        this.audit = audit;
    }

    public Data(String tableName, DataEventType eventType, String rowData, String pkData, TriggerHistory audit) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowData = rowData;
        this.pkData = pkData;
        this.audit = audit;
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

    public TriggerHistory getAudit() {
        return audit;
    }

    public void setAudit(TriggerHistory audit) {
        this.audit = audit;
    }

}
