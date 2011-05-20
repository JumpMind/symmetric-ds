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

package org.jumpmind.symmetric.core.model;

import java.io.Serializable;
import java.util.Date;

/**
 * Maps to the table sync audit table which tracks the history of sync trigger
 * creation.
 * <p/>
 * This table also tracks the columns and the primary keys as of the create date
 * so that if the table definition changes while we still have events to process
 * (as may be the case when distributing events to remote locations), then we
 * still have the history of what the columns and primary keys were at the time.
 * 
 * 
 */
public class TriggerHistory extends AbstractCsvData implements Serializable {

    private static final long serialVersionUID = 1L;

    private int triggerHistoryId;

    private Trigger trigger;

    private String sourceTableName;

    private String sourceSchemaName;

    private String sourceCatalogName;

    private Date createTime;

    private String columnNames;

    private String pkColumnNames;

    private String nameForInsertTrigger;

    private String nameForUpdateTrigger;

    private String nameForDeleteTrigger;

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

    public TriggerHistory(String tableName, String pkColumnNames, String columnNames) {
        this.sourceTableName = tableName;
        this.pkColumnNames = pkColumnNames;
        this.columnNames = columnNames;
    }

    public TriggerHistory(Table table, Trigger trigger) {
        this(table, trigger, null);
    }

    public TriggerHistory(Table table, Trigger trigger, TriggerReBuildReason reason) {
        this();
        this.sourceTableName = table.getTableName();
        this.lastTriggerBuildReason = reason;
        this.columnNames = getCommaDeliminatedColumns(trigger.orderColumnsForTable(table));
        this.sourceSchemaName = trigger.getSourceSchemaName();
        this.sourceCatalogName = trigger.getSourceCatalogName();
        this.trigger = trigger;
        this.pkColumnNames = getCommaDeliminatedColumns(table.getPrimaryKeyColumnsArray());
        this.triggerRowHash = trigger.toHashedValue();
        // set primary key equal to all the columns to make data sync work for
        // tables with no primary keys
        if (pkColumnNames == null) {
            pkColumnNames = columnNames;
        }

        tableHash = calculateTableHashFor(table);
    }

    public static int calculateTableHashFor(Table table) {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + table.getTableName().hashCode();
        result = PRIME * result + calculateHashForColumns(PRIME, table.getColumns());
        result = PRIME * result + calculateHashForColumns(PRIME, table.getPrimaryKeyColumnsArray());
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

    public String[] getParsedColumnNames() {
        return getData("columnNames", columnNames);
    }

    public String[] getParsedPkColumnNames() {
        return getData("pkColumnNames", pkColumnNames);
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

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
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

}