/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.DataEventType;

/**
 * Maps to the table sync audit table which tracks the history of sync trigger
 * creation.
 * <p/>
 * This table also tracks the columns and the primary keys as of the create date
 * so that if the table definition changes while we still have events to process
 * (as may be the case when distributing events to remote locations), then we
 * still have the history of what the columns and primary keys were at the time.
 */
public class TriggerHistory implements Serializable {

    private static final long serialVersionUID = 1L;

    private int triggerHistoryId;

    private String triggerId;

    private String sourceTableName;

    private String sourceSchemaName;

    private String sourceCatalogName;

    private Date createTime;

    private String columnNames;
    
    private String[] parsedColumnNames;

    private String pkColumnNames;
    
    private String[] parsedPkColumnNames;

    private String nameForInsertTrigger;

    private String nameForUpdateTrigger;

    private String nameForDeleteTrigger;

    private String errorMessage;

    private Date inactiveTime;

    /**
     * This is a hash based on the tablename, column names, and column data
     * types. It is used to effectively version a table so we know when it
     * changes.
     */
    private int tableHash;

    /**
     * This is a hash based on the values in the trigger configuration table.
     */
    private long triggerRowHash;

    private TriggerReBuildReason lastTriggerBuildReason;

    public TriggerHistory() {
        createTime = new Date();
    }

    public TriggerHistory(int triggerHistoryId) {
        this();
        this.triggerHistoryId = triggerHistoryId;
    }

    public TriggerHistory(String tableName, String pkColumnNames, String columnNames) {
        this();
        this.sourceTableName = tableName;
        this.pkColumnNames = pkColumnNames;
        this.columnNames = columnNames;
    }

    public TriggerHistory(Table table, Trigger trigger) {
        this(table, trigger, null);
    }

    public TriggerHistory(Table table, Trigger trigger, TriggerReBuildReason reason) {
        this();        
        this.lastTriggerBuildReason = reason;
        this.sourceTableName = trigger.getSourceTableName();
        this.columnNames = getCommaDeliminatedColumns(trigger.orderColumnsForTable(table));
        this.sourceSchemaName = trigger.getSourceSchemaName();
        this.sourceCatalogName = trigger.getSourceCatalogName();
        this.triggerId = trigger.getTriggerId();
        this.pkColumnNames = getCommaDeliminatedColumns(table.getPrimaryKeyColumns());
        this.triggerRowHash = trigger.toHashedValue();
        tableHash = table.calculateTableHashcode();
    }
    
    public TriggerHistory(Trigger trigger) {
        this.sourceCatalogName = trigger.getSourceCatalogName();
        this.sourceSchemaName = trigger.getSourceSchemaName();
        this.sourceTableName = trigger.getSourceTableName();
        this.triggerId = trigger.getTriggerId();
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
            return " ";
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

    public String[] getParsedColumnNames() {
        if (parsedColumnNames == null && columnNames != null) {
            parsedColumnNames = columnNames.split(",");
        }
        return parsedColumnNames;
    }

    public String[] getParsedPkColumnNames() {
        if (parsedPkColumnNames == null && pkColumnNames != null) {
            parsedPkColumnNames = pkColumnNames.split(",");
        }
        return parsedPkColumnNames;
    }

    public int getTableHash() {
        return tableHash;
    }

    public void setTableHash(int tableHash) {
        this.tableHash = tableHash;
    }

    public String getSourceTableName() {
        return sourceTableName;
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

    public void setLastTriggerBuildReason(TriggerReBuildReason lastTriggerBuildReason) {
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

    public String getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(String triggerId) {
        this.triggerId = triggerId;
    }

    public Date getInactiveTime() {
        return inactiveTime;
    }

    public void setInactiveTime(Date inactiveTime) {
        this.inactiveTime = inactiveTime;
    }

    public String getSourceCatalogName() {
        return sourceCatalogName;
    }

    public void setSourceCatalogName(String sourceCatalogName) {
        this.sourceCatalogName = sourceCatalogName;
    }

    public long getTriggerRowHash() {
        return triggerRowHash;
    }

    public void setTriggerRowHash(long triggerRowHash) {
        this.triggerRowHash = triggerRowHash;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public int toVirtualTriggerHistId() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columnNames == null) ? 0 : columnNames.hashCode());
        result = prime * result + ((sourceCatalogName == null) ? 0 : sourceCatalogName.hashCode());
        result = prime * result + ((sourceSchemaName == null) ? 0 : sourceSchemaName.hashCode());
        result = prime * result + ((sourceTableName == null) ? 0 : sourceTableName.hashCode());
        return result;
    }
    
}