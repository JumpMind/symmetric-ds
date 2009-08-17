/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *               Eric Long <erilong@users.sourceforge.net>
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;

/**
 * Defines the trigger via which a table will be synchronized.
 */
public class Trigger {

    static final Log logger = LogFactory.getLog(Trigger.class);

    private static int maxTriggerId;
    
    private static final long serialVersionUID = 8947288471097851573L;

    private static final String DEFAULT_CONDITION = "1=1";

    private int triggerId;

    private String sourceTableName;

    private String sourceSchemaName;

    private String sourceCatalogName;

    private String channelId;

    private boolean syncOnUpdate = true;

    private boolean syncOnInsert = true;

    private boolean syncOnDelete = true;

    private boolean syncOnIncomingBatch = false;

    private boolean syncColumnLevel = false;

    private String nameForInsertTrigger;

    private String nameForUpdateTrigger;

    private String nameForDeleteTrigger;

    private String syncOnUpdateCondition = DEFAULT_CONDITION;

    private String syncOnInsertCondition = DEFAULT_CONDITION;

    private String syncOnDeleteCondition = DEFAULT_CONDITION;

    private String excludedColumnNames = null;

    /**
     * This is a sql expression that creates a unique id which the sync process
     * can use to 'group' events together and commit together.
     */
    private String txIdExpression = null;

    private Date inactiveTime;

    private Date createTime;

    private Date lastUpdateTime;

    private String lastUpdateBy;

    public Trigger() {
        triggerId = maxTriggerId++;
    }

    public Trigger(String tableName) {
        this.sourceTableName = tableName;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public void setCreateTime(Date createdOn) {
        this.createTime = createdOn;
    }

    public void setLastUpdateTime(Date lastModifiedOn) {
        this.lastUpdateTime = lastModifiedOn;
    }

    public void setLastUpdateBy(String updatedBy) {
        this.lastUpdateBy = updatedBy;
    }

    /**
     * When dealing with columns, always use this method to order the columns so
     * that the primary keys are first.
     */
    public Column[] orderColumnsForTable(Table table) {
        List<String> excludedColumnNames = getExcludedColumnNamesAsList();
        Column[] pks = table.getPrimaryKeyColumns();
        Column[] cols = table.getColumns();
        List<Column> orderedColumns = new ArrayList<Column>(cols.length);
        for (int i = 0; i < pks.length; i++) {
            orderedColumns.add(pks[i]);
        }
        for (int i = 0; i < cols.length; i++) {
            Column col = cols[i];
            if (!col.isPrimaryKey() && !excludedColumnNames.contains(col.getName().toLowerCase())) {
                orderedColumns.add(col);
            }
        }
        return orderedColumns.toArray(new Column[orderedColumns.size()]);
    }

    /**
     * Get a list of the natural indexes of the excluded columns
     */
    public int[] getExcludedColumnIndexes(Table table) {
        if (excludedColumnNames != null && excludedColumnNames.length() > 0) {
            StringTokenizer tokenizer = new StringTokenizer(excludedColumnNames, ",");
            int[] indexes = new int[tokenizer.countTokens()];
            Column[] columns = table.getColumns();
            List<String> columnNames = new ArrayList<String>(columns.length);
            for (Column column : columns) {
                columnNames.add(column.getName().toLowerCase());
            }
            int i = 0;
            while (tokenizer.hasMoreTokens()) {
                indexes[i++] = columnNames.indexOf(tokenizer.nextToken().toLowerCase());
            }
            return indexes;
        } else {
            return new int[0];
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> getExcludedColumnNamesAsList() {
        if (excludedColumnNames != null && excludedColumnNames.length() > 0) {
            StringTokenizer tokenizer = new StringTokenizer(excludedColumnNames, ",");
            List<String> columnNames = new ArrayList<String>(tokenizer.countTokens());
            while (tokenizer.hasMoreTokens()) {
                columnNames.add(tokenizer.nextToken().toLowerCase());
            }
            return columnNames;
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    public boolean hasChangedSinceLastTriggerBuild(Date lastTriggerBuildTime) {
        return lastTriggerBuildTime == null || getLastUpdateTime() == null
                || lastTriggerBuildTime.before(getLastUpdateTime());
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isSyncOnDelete() {
        return syncOnDelete;
    }

    public void setSyncOnDelete(boolean syncOnDelete) {
        this.syncOnDelete = syncOnDelete;
    }

    public String getSyncOnDeleteCondition() {
        return syncOnDeleteCondition;
    }

    public void setSyncOnDeleteCondition(String syncOnDeleteCondition) {
        this.syncOnDeleteCondition = syncOnDeleteCondition;
    }

    public boolean isSyncOnInsert() {
        return syncOnInsert;
    }

    public void setSyncOnInsert(boolean syncOnInsert) {
        this.syncOnInsert = syncOnInsert;
    }

    public String getSyncOnInsertCondition() {
        return syncOnInsertCondition;
    }

    public void setSyncOnInsertCondition(String syncOnInsertCondition) {
        this.syncOnInsertCondition = syncOnInsertCondition;
    }

    public boolean isSyncOnUpdate() {
        return syncOnUpdate;
    }

    public void setSyncOnUpdate(boolean syncOnUpdate) {
        this.syncOnUpdate = syncOnUpdate;
    }

    public String getSyncOnUpdateCondition() {
        return syncOnUpdateCondition;
    }

    public void setSyncOnUpdateCondition(String syncOnUpdateCondition) {
        this.syncOnUpdateCondition = syncOnUpdateCondition;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String tableName) {
        this.sourceTableName = tableName;
    }

    public String getTxIdExpression() {
        return txIdExpression;
    }

    public void setTxIdExpression(String batchIdExpression) {
        this.txIdExpression = batchIdExpression;
    }

    public String getSourceSchemaName() {
        return sourceSchemaName;
    }

    public void setSourceSchemaName(String schemaName) {
        this.sourceSchemaName = schemaName;
    }

    public String getExcludedColumnNames() {
        return excludedColumnNames;
    }

    public void setExcludedColumnNames(String excludeColumnNames) {
        this.excludedColumnNames = excludeColumnNames;
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

    public int getTriggerId() {
        return triggerId;
    }

    public void setTriggerId(int triggerId) {        
        this.triggerId = triggerId;
        if (triggerId >= maxTriggerId) {
            maxTriggerId = triggerId+1;
        }
    }

    public Date getInactiveTime() {
        return inactiveTime;
    }

    public void setInactiveTime(Date inactiveTime) {
        this.inactiveTime = inactiveTime;
    }

    public boolean isSyncOnIncomingBatch() {
        return syncOnIncomingBatch;
    }

    public void setSyncOnIncomingBatch(boolean syncOnIncomingBatch) {
        this.syncOnIncomingBatch = syncOnIncomingBatch;
    }

    public String getSourceCatalogName() {
        return sourceCatalogName;
    }

    public void setSourceCatalogName(String sourceCatalogName) {
        this.sourceCatalogName = sourceCatalogName;
    }

    public boolean isSyncColumnLevel() {
        return syncColumnLevel;
    }

    public void setSyncColumnLevel(boolean syncColumnLevel) {
        this.syncColumnLevel = syncColumnLevel;
    }

    public long getHashedValue() {
        long hashedValue = triggerId;
        if (null != sourceTableName) {
            hashedValue += sourceTableName.hashCode();
        }

        if (null != channelId) {
            hashedValue += channelId.hashCode();
        }

        if (null != sourceSchemaName) {
            hashedValue += sourceSchemaName.hashCode();
        }

        if (null != sourceCatalogName) {
            hashedValue += sourceCatalogName.hashCode();
        }

        hashedValue += syncOnUpdate ? 1 : 0;
        hashedValue += syncOnInsert ? 1 : 0;
        hashedValue += syncOnDelete ? 1 : 0;
        hashedValue += syncOnIncomingBatch ? 1 : 0;
        hashedValue += syncColumnLevel ? 1 : 0;

        if (null != nameForInsertTrigger) {
            hashedValue += nameForInsertTrigger.hashCode();
        }

        if (null != nameForUpdateTrigger) {
            hashedValue += nameForUpdateTrigger.hashCode();
        }

        if (null != nameForDeleteTrigger) {
            hashedValue += nameForDeleteTrigger.hashCode();
        }

        if (null != syncOnUpdateCondition) {
            hashedValue += syncOnUpdateCondition.hashCode();
        }

        if (null != syncOnInsertCondition) {
            hashedValue += syncOnInsertCondition.hashCode();
        }

        if (null != syncOnDeleteCondition) {
            hashedValue += syncOnDeleteCondition.hashCode();
        }

        if (null != excludedColumnNames) {
            hashedValue += excludedColumnNames.hashCode();
        }

        if (null != txIdExpression) {
            hashedValue += txIdExpression.hashCode();
        }

        return hashedValue;
    }

    public boolean isSame(Trigger trigger) {
        return isSame(sourceCatalogName, trigger.sourceCatalogName)
                && isSame(sourceSchemaName, trigger.sourceSchemaName)
                && trigger.sourceTableName.equalsIgnoreCase(sourceTableName);
    }

    protected boolean isSame(String one, String two) {
        return (one == null && two == null) || (one != null && two != null && one.equals(two));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Trigger) {
            return triggerId == ((Trigger) obj).triggerId;

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return triggerId;
    }

}
