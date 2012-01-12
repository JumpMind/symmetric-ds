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
 * under the License.  */


package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;

/**
 * Defines the trigger via which a table will be synchronized.
 */
public class Trigger implements Serializable {

    private static final long serialVersionUID = 1L;

    static final Log logger = LogFactory.getLog(Trigger.class);

    private static int maxTriggerId;

    private static final String DEFAULT_CONDITION = "1=1";

    private String triggerId;

    private String sourceTableName;

    private String sourceSchemaName;

    private String sourceCatalogName;

    private String channelId;

    private boolean syncOnUpdate = true;

    private boolean syncOnInsert = true;

    private boolean syncOnDelete = true;

    private boolean syncOnIncomingBatch = false;
    
    private boolean useStreamLobs = false;
    
    private boolean useCaptureLobs = false;

    private String nameForInsertTrigger;

    private String nameForUpdateTrigger;

    private String nameForDeleteTrigger;

    private String syncOnUpdateCondition = DEFAULT_CONDITION;

    private String syncOnInsertCondition = DEFAULT_CONDITION;

    private String syncOnDeleteCondition = DEFAULT_CONDITION;

    private String excludedColumnNames = null;

    /**
     * This is a SQL expression that creates a unique id which the sync process
     * can use to 'group' events together and commit together.
     */
    private String txIdExpression = null;
    
    private String externalSelect = null;

    private Date createTime;

    private Date lastUpdateTime;

    private String lastUpdateBy;

    public Trigger() {
        triggerId = Integer.toString(maxTriggerId++);
    }

    public Trigger(String tableName) {
        this.sourceTableName = tableName;
    }
    
    final public String qualifiedSourceTableName() {
        return qualifiedSourceTablePrefix() + sourceTableName;
    }
    
    final public String qualifiedSourceTablePrefix() {
        String schemaPlus = (getSourceSchemaName() != null ? getSourceSchemaName()
                + "." : "");
        String catalogPlus = (getSourceCatalogName() != null ? getSourceCatalogName()
                + "." : "") + schemaPlus;
        return catalogPlus;
    }
    
    public void nullOutBlankFields() {
        if (StringUtils.isBlank(sourceCatalogName)) {
            sourceCatalogName = null;
        } 
        if (StringUtils.isBlank(sourceSchemaName)) {
            sourceSchemaName = null;
        } 
    }

    /**
     * When dealing with columns, always use this method to order the columns so
     * that the primary keys are first.
     */
    public Column[] orderColumnsForTable(Table table) {
        if (table != null) {
            List<String> excludedColumnNames = getExcludedColumnNamesAsList();
            Column[] pks = table.getPrimaryKeyColumns();
            Column[] cols = table.getColumns();
            List<Column> orderedColumns = new ArrayList<Column>(cols.length);
            for (int i = 0; i < pks.length; i++) {
                orderedColumns.add(pks[i]);
            }
            for (int i = 0; i < cols.length; i++) {
                Column col = cols[i];
                if (!col.isPrimaryKey()
                        && !excludedColumnNames.contains(col.getName().toLowerCase())) {
                    orderedColumns.add(col);
                }
            }
            return orderedColumns.toArray(new Column[orderedColumns.size()]);
        } else {
            return new Column[0];
        }
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
                columnNames.add(tokenizer.nextToken().toLowerCase().trim());
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


    public String getTriggerId() {
        return triggerId;
    }
    
    public void setTriggerId(String triggerId) {
        this.triggerId = triggerId;
        if (StringUtils.isNotBlank(triggerId) && StringUtils.isNumeric(triggerId)) {
            int id = Integer.parseInt(triggerId);
            if (id >= maxTriggerId) {
                maxTriggerId = id + 1;
            }
        }
    }    
    

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getSourceSchemaName() {
        return sourceSchemaName;
    }

    public void setSourceSchemaName(String sourceSchemaName) {
        this.sourceSchemaName = sourceSchemaName;
    }

    public String getSourceCatalogName() {
        return sourceCatalogName;
    }

    public void setSourceCatalogName(String sourceCatalogName) {
        this.sourceCatalogName = sourceCatalogName;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public boolean isSyncOnUpdate() {
        return syncOnUpdate;
    }

    public void setSyncOnUpdate(boolean syncOnUpdate) {
        this.syncOnUpdate = syncOnUpdate;
    }

    public boolean isSyncOnInsert() {
        return syncOnInsert;
    }

    public void setSyncOnInsert(boolean syncOnInsert) {
        this.syncOnInsert = syncOnInsert;
    }

    public boolean isSyncOnDelete() {
        return syncOnDelete;
    }

    public void setSyncOnDelete(boolean syncOnDelete) {
        this.syncOnDelete = syncOnDelete;
    }

    public boolean isSyncOnIncomingBatch() {
        return syncOnIncomingBatch;
    }

    public void setSyncOnIncomingBatch(boolean syncOnIncomingBatch) {
        this.syncOnIncomingBatch = syncOnIncomingBatch;
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

    public String getNameForDeleteTrigger() {
        return nameForDeleteTrigger;
    }

    public void setNameForDeleteTrigger(String nameForDeleteTrigger) {
        this.nameForDeleteTrigger = nameForDeleteTrigger;
    }

    public String getSyncOnUpdateCondition() {
        return syncOnUpdateCondition;
    }

    public void setSyncOnUpdateCondition(String syncOnUpdateCondition) {
        this.syncOnUpdateCondition = syncOnUpdateCondition;
    }

    public String getSyncOnInsertCondition() {
        return syncOnInsertCondition;
    }

    public void setSyncOnInsertCondition(String syncOnInsertCondition) {
        this.syncOnInsertCondition = syncOnInsertCondition;
    }

    public String getSyncOnDeleteCondition() {
        return syncOnDeleteCondition;
    }

    public void setSyncOnDeleteCondition(String syncOnDeleteCondition) {
        this.syncOnDeleteCondition = syncOnDeleteCondition;
    }

    public String getExcludedColumnNames() {
        return excludedColumnNames;
    }

    public void setExcludedColumnNames(String excludedColumnNames) {
        this.excludedColumnNames = excludedColumnNames;
    }

    public String getTxIdExpression() {
        return txIdExpression;
    }

    public void setTxIdExpression(String txIdExpression) {
        this.txIdExpression = txIdExpression;
    }           

    public String getExternalSelect() {
        return externalSelect;
    }

    public void setExternalSelect(String externalSelect) {
        this.externalSelect = externalSelect;
    }

    public void setLastUpdateBy(String updatedBy) {
        this.lastUpdateBy = updatedBy;
    }
    
    public String getLastUpdateBy() {
        return lastUpdateBy;
    }

    public Date getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Date lastModifiedOn) {
        this.lastUpdateTime = lastModifiedOn;
    }
    
    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createdOn) {
        this.createTime = createdOn;
    }
    
    public void setUseStreamLobs(boolean useStreamLobs) {
        this.useStreamLobs = useStreamLobs;
    }
        
    public boolean isUseStreamLobs() {
        return useStreamLobs;
    }
    
    public void setUseCaptureLobs(boolean useCaptureLobs) {
        this.useCaptureLobs = useCaptureLobs;
    }
    
    public boolean isUseCaptureLobs() {
        return useCaptureLobs;
    }
    
    public long toHashedValue() {
        long hashedValue = triggerId != null ? triggerId.hashCode() : 0;
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

        hashedValue += syncOnUpdate ? "syncOnUpdate".hashCode() : 0;
        hashedValue += syncOnInsert ? "syncOnInsert".hashCode() : 0;
        hashedValue += syncOnDelete ? "syncOnDelete".hashCode() : 0;
        hashedValue += syncOnIncomingBatch ? "syncOnIncomingBatch".hashCode() : 0;
        hashedValue += useStreamLobs ? "useStreamLobs".hashCode() : 0;
        hashedValue += useCaptureLobs ? "useCaptureLobs".hashCode() : 0;

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
        if (obj instanceof Trigger && triggerId != null) {
            return triggerId.equals(((Trigger) obj).triggerId);
        } else {
            return super.equals(obj);
        }
    }

    @Override
    public int hashCode() {
        return triggerId != null ? triggerId.hashCode() : super.hashCode();
    }
    
    @Override
    public String toString() {
        if (triggerId != null) {
            return triggerId;
        } else {
            return super.toString();
        }
    }

}