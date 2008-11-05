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

    private static final String DEFAULT_SYMMETRIC_TABLE_PREFIX = "SYM";

    private static final long serialVersionUID = 8947288471097851573L;

    private static final String DEFAULT_CONDITION = "1=1";

    private int triggerId;

    private String sourceTableName;

    private String targetTableName;

    private String sourceGroupId;

    private String channelId;

    private String targetGroupId;

    private String sourceSchemaName;

    private String sourceCatalogName;

    private String targetSchemaName;

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
     * Allows the end user to limit the data loaded when doing the initial load.
     * if null, will default to 'from tablename'
     */
    private String initialLoadSelect = DEFAULT_CONDITION;

    /**
     * Default to selecting all. This can be changed to select based on joins
     * between node parameters and data column values.
     */
    private String nodeSelect = "";

    /**
     * This is a sql expression that creates a unique id which the sync process
     * can use to 'group' events together and commit together.
     */
    private String txIdExpression = null;

    /**
     * This is the order in which the definitions will be processed.
     */
    private int initialLoadOrder;

    private Date inactiveTime;

    private Date createdOn;

    private Date lastModifiedTime;

    private String updatedBy;

    public Trigger() {
    }

    public Trigger(String tableName, boolean syncOnUpdate, boolean syncOnInsert, boolean syncOnDelete,
            String configurationId, String channelId, String syncOnUpdateCondition, String syncOnInsertCondition,
            String syncOnDeleteCondition) {
        this.sourceTableName = tableName;
        this.syncOnUpdate = syncOnUpdate;
        this.syncOnInsert = syncOnInsert;
        this.syncOnDelete = syncOnDelete;
        this.sourceGroupId = configurationId;
        this.channelId = channelId;
        this.syncOnUpdateCondition = syncOnUpdateCondition;
        this.syncOnInsertCondition = syncOnInsertCondition;
        this.syncOnDeleteCondition = syncOnDeleteCondition;
    }

    public Date getCreatedOn() {
        return createdOn;
    }

    public Date getLastModifiedTime() {
        return lastModifiedTime;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setCreatedOn(Date createdOn) {
        this.createdOn = createdOn;
    }

    public void setLastModifiedTime(Date lastModifiedOn) {
        this.lastModifiedTime = lastModifiedOn;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
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

    public String getTriggerName(DataEventType dml, String triggerPrefix, int maxTriggerNameLength) {
        String triggerName = null;
        if (triggerPrefix == null) {
            triggerPrefix = "";
        }
        switch (dml) {
        case INSERT:
            if (nameForInsertTrigger != null) {
                triggerName = getNameForInsertTrigger();
            }
            break;
        case UPDATE:
            if (nameForUpdateTrigger != null) {
                triggerName = getNameForUpdateTrigger();
            }
            break;
        case DELETE:
            if (nameForDeleteTrigger != null) {
                triggerName = getNameForDeleteTrigger();
            }
            break;
        }
        if (triggerName == null) {
            triggerName = triggerPrefix + "on_" + dml.getCode().toLowerCase() + "_to_" + getShortTableName();
        }

        if (triggerName.length() > maxTriggerNameLength && maxTriggerNameLength > 0) {
            triggerName = triggerName.substring(0, maxTriggerNameLength - 1);
            logger.warn("We just truncated the trigger name for the " + dml.name().toLowerCase() + " trigger id="
                    + triggerId
                    + ".  You might want to consider manually providing a name for the trigger that is les than "
                    + maxTriggerNameLength + " characters long.");
        }
        return triggerName;
    }

    private String getShortTableName() {
        StringBuilder shortName = new StringBuilder();
        String table = getSourceTableName();
        if (table.toUpperCase().startsWith(DEFAULT_SYMMETRIC_TABLE_PREFIX)) {
            table = table.substring(DEFAULT_SYMMETRIC_TABLE_PREFIX.length() + 1);
        }
        CharSequence seq = table;
        char previousChar = ' ';
        for (int i = 0; i < seq.length(); i++) {
            char c = seq.charAt(i);
            if (i == 0
                    || !(c == previousChar || c == 'y' || c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u'
                            || c == 'Y' || c == 'A' || c == 'E' || c == 'I' || c == 'O' || c == 'U')) {
                shortName.append(c);
            }
            previousChar = c;
        }
        return shortName.toString();
    }

    public boolean hasChangedSinceLastTriggerBuild(Date lastTriggerBuildTime) {
        return lastTriggerBuildTime == null || getLastModifiedTime() == null
                || lastTriggerBuildTime.before(getLastModifiedTime());
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getSourceGroupId() {
        return sourceGroupId;
    }

    public void setSourceGroupId(String domainName) {
        this.sourceGroupId = domainName;
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

    public String getNodeSelect() {
        return nodeSelect;
    }

    public void setNodeSelect(String registrantSelect) {
        this.nodeSelect = registrantSelect;
    }

    public String getTxIdExpression() {
        return txIdExpression;
    }

    public void setTxIdExpression(String batchIdExpression) {
        this.txIdExpression = batchIdExpression;
    }

    public String getInitialLoadSelect() {
        return initialLoadSelect;
    }

    public void setInitialLoadSelect(String initialLoadExpression) {
        this.initialLoadSelect = initialLoadExpression;
    }

    public int getInitialLoadOrder() {
        return initialLoadOrder;
    }

    public void setInitialLoadOrder(int order) {
        this.initialLoadOrder = order;
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

    public String getTargetGroupId() {
        return targetGroupId;
    }

    public void setTargetGroupId(String targetDomainName) {
        this.targetGroupId = targetDomainName;
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
    }

    public Date getInactiveTime() {
        return inactiveTime;
    }

    public void setInactiveTime(Date inactiveTime) {
        this.inactiveTime = inactiveTime;
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

}
