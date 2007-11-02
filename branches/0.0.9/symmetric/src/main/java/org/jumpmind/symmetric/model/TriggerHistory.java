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

import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;

/**
 * Maps to the table sync audit table which tracks the history of sync trigger creation.
 * <p/>
 * This table also tracks the columns and the primary keys as of the create date so that if the table
 * definition changes while we still have events to process (as may be the case when distributing events
 * to remote locations), then we still have the history of what the columns and primary keys were at the 
 * time.
 */
public class TriggerHistory {

    private int triggerHistoryId;
    
    private int triggerId;

    private String sourceTableName;
    
    private String sourceSchemaName;

    private Date createTime;

    private String columnNames;

    private String pkColumnNames;

    private String nameForInsertTrigger;

    private String nameForUpdateTrigger;

    private String nameForDeleteTrigger;
    
    private Date inactiveTime;
    

    /**
     * This is a hash based on the tablename, column names, and column data types.  It is used to 
     * effectively version a table so we know when it changes.
     */
    private int tableHash;

    private TriggerReBuildReason lastTriggerBuildReason;

    public TriggerHistory() {
        createTime = new Date();
    }

    public TriggerHistory(String tableName, String pkColumnNames,
            String columnNames) {
        this.sourceTableName = tableName;
        this.pkColumnNames = pkColumnNames;
        this.columnNames = columnNames;
    }

    public TriggerHistory(Table table, Trigger trigger,
            TriggerReBuildReason reason, String nameForInsertTrigger, String nameForUpdateTrigger, String nameForDeleteTrigger) {
        this();
        this.sourceTableName = table.getName();
        this.lastTriggerBuildReason = reason;
        this.nameForDeleteTrigger = nameForDeleteTrigger;
        this.nameForInsertTrigger = nameForInsertTrigger;
        this.nameForUpdateTrigger = nameForUpdateTrigger;
        this.columnNames = getCommaDeliminatedColumns(trigger
                .orderColumnsForTable(table));
        this.sourceSchemaName = trigger.getSourceSchemaName();
        this.triggerId = trigger.getTriggerId();
        this.pkColumnNames = getCommaDeliminatedColumns(table.getPrimaryKeyColumns());
        // set primary key equal to all the columns to make data sync work for tables
        // with no primary keys
        if (pkColumnNames == null) {
            pkColumnNames = columnNames;
        }

        tableHash = calculateTableHashFor(table);
    }

    public static int calculateTableHashFor(Table table) {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + table.getName().hashCode();
        result = PRIME * result
                + calculateHashForColumns(PRIME, table.getColumns());
        result = PRIME * result
                + calculateHashForColumns(PRIME, table.getPrimaryKeyColumns());
        return result;
    }

    private static int calculateHashForColumns(final int PRIME, Column[] cols) {
        int result = 1;
        if (cols != null && cols.length > 0) {
            for (Column column : cols) {
                result = PRIME * result + column.getName().hashCode();
                result = PRIME * result + column.getType().hashCode();
                result = PRIME * result + column.getSizeAsInt();
            }
        }
        return result;
    }

    private String getCommaDeliminatedColumns(Column[] cols) {
        StringBuilder columns = new StringBuilder();
        if (cols != null && cols.length > 0) {
            for (Column column : cols) {
                columns.append(column.getName());
                columns.append(",");
            }
            columns.replace(columns.length() - 1, columns.length(), "");
            return columns.toString();
        } else {
            return null;
        }
    } 
    
    public String getTriggerNameForDmlType(DataEventType type) {
        switch (type) {
        case INSERT:
            return getNameForInsertTrigger();
        case UPDATE:
            return getNameForUpdateTrigger();
        case DELETE:
            return getNameForDeleteTrigger();
        }
        throw new IllegalStateException();
    }

    public int getTableHash() {
        return tableHash;
    }

    public void setTableHash(int tableHash) {
        this.tableHash = tableHash;
    }

    public String getSourceTableName() {
        return sourceTableName.toUpperCase();
    }

    public void setSourceTableName(String tableName) {
        this.sourceTableName = tableName;
    }

    public String getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String allColumnData) {
        this.columnNames = allColumnData;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public TriggerReBuildReason getLastTriggerBuildReason() {
        return lastTriggerBuildReason;
    }

    public void setLastTriggerBuildReason(
            TriggerReBuildReason lastTriggerBuildReason) {
        this.lastTriggerBuildReason = lastTriggerBuildReason;
    }

    public String getPkColumnNames() {
        return pkColumnNames;
    }

    public void setPkColumnNames(String pkColumnData) {
        this.pkColumnNames = pkColumnData;
    }

    public int getTriggerHistoryId() {
        return triggerHistoryId;
    }

    public void setTriggerHistoryId(int tableSyncAuditId) {
        this.triggerHistoryId = tableSyncAuditId;
    }

    public String getNameForDeleteTrigger() {
        return nameForDeleteTrigger;
    }

    public void setNameForDeleteTrigger(String nameForDeleteTrigger) {
        this.nameForDeleteTrigger = nameForDeleteTrigger;
    }

    public String getNameForInsertTrigger() {
        return nameForInsertTrigger;
    }

    public void setNameForInsertTrigger(String nameForInsertTrigger) {
        this.nameForInsertTrigger = nameForInsertTrigger;
    }

    public String getNameForUpdateTrigger() {
        return nameForUpdateTrigger;
    }

    public void setNameForUpdateTrigger(String nameForUpdateTrigger) {
        this.nameForUpdateTrigger = nameForUpdateTrigger;
    }

    public String getSourceSchemaName() {
        return sourceSchemaName;
    }

    public void setSourceSchemaName(String schemaName) {
        this.sourceSchemaName = schemaName;
    }

    public int getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(int triggerId) {
        this.triggerId = triggerId;
    }

    public Date getInactiveTime() {
        return inactiveTime;
    }

    public void setInactiveTime(Date inactiveTime) {
        this.inactiveTime = inactiveTime;
    }

}
