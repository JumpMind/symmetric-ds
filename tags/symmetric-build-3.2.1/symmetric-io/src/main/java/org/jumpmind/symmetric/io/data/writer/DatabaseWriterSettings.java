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
package org.jumpmind.symmetric.io.data.writer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;

public class DatabaseWriterSettings {

    protected long maxRowsBeforeCommit = 10000;

    protected boolean treatDateTimeFieldsAsVarchar = false;

    protected boolean usePrimaryKeysFromSource = true;

    protected Conflict defaultConflictSetting;
    
    protected boolean createTableFailOnError = true;
    
    protected boolean alterTable = true;
    
    protected boolean createTableDropFirst = false;
    
    protected boolean createTableAlterCaseToMatchDatabaseDefault = false;
    
    protected boolean ignoreMissingTables = true;

    protected Map<String, Conflict> conflictSettingsByChannel;

    protected Map<String, Conflict> conflictSettingsByTable;

    protected List<IDatabaseWriterFilter> databaseWriterFilters;
    
    protected List<IDatabaseWriterErrorHandler> databaseWriterErrorHandlers;
    
    protected List<ResolvedData> resolvedData;
    
    public boolean isAlterTable() {
        return alterTable;
    }
    
    public void setAlterTable(boolean alterTable) {
        this.alterTable = alterTable;
    }
    
    public boolean isCreateTableDropFirst() {
        return createTableDropFirst;
    }
    
    public void setCreateTableDropFirst(boolean createTableDropFirst) {
        this.createTableDropFirst = createTableDropFirst;
    }
    
    public boolean isCreateTableFailOnError() {
        return createTableFailOnError;
    }
    
    public void setCreateTableFailOnError(boolean createTableFailOnError) {
        this.createTableFailOnError = createTableFailOnError;
    }

    public long getMaxRowsBeforeCommit() {
        return maxRowsBeforeCommit;
    }

    public void setMaxRowsBeforeCommit(long maxRowsBeforeCommit) {
        this.maxRowsBeforeCommit = maxRowsBeforeCommit;
    }

    public boolean isTreatDateTimeFieldsAsVarchar() {
        return treatDateTimeFieldsAsVarchar;
    }

    public void setTreatDateTimeFieldsAsVarchar(boolean treatDateTimeFieldsAsVarchar) {
        this.treatDateTimeFieldsAsVarchar = treatDateTimeFieldsAsVarchar;
    }

    public boolean isUsePrimaryKeysFromSource() {
        return usePrimaryKeysFromSource;
    }

    public void setUsePrimaryKeysFromSource(boolean usePrimaryKeysFromSource) {
        this.usePrimaryKeysFromSource = usePrimaryKeysFromSource;
    }

    public Conflict getDefaultConflictSetting() {
        return defaultConflictSetting;
    }

    public void setDefaultConflictSetting(Conflict defaultConflictSetting) {
        this.defaultConflictSetting = defaultConflictSetting;
    }
    
    public boolean isCreateTableAlterCaseToMatchDatabaseDefault() {
        return createTableAlterCaseToMatchDatabaseDefault;
    }
    
    public void setCreateTableAlterCaseToMatchDatabaseDefault(
            boolean createTableAlterCaseToMatchDatabaseDefault) {
        this.createTableAlterCaseToMatchDatabaseDefault = createTableAlterCaseToMatchDatabaseDefault;
    }

    public Map<String, Conflict> getConflictSettingsByChannel() {
        return conflictSettingsByChannel;
    }

    public void setConflictSettingsByChannel(Map<String, Conflict> conflictSettingsByChannel) {
        this.conflictSettingsByChannel = conflictSettingsByChannel;
    }

    public Map<String, Conflict> getConflictSettingsByTable() {
        return conflictSettingsByTable;
    }

    public void setConflictSettingsByTable(Map<String, Conflict> conflictSettingsByTable) {
        this.conflictSettingsByTable = conflictSettingsByTable;
    }

    public List<IDatabaseWriterFilter> getDatabaseWriterFilters() {
        return databaseWriterFilters;
    }

    public void setDatabaseWriterFilters(List<IDatabaseWriterFilter> databaseWriterFilters) {
        this.databaseWriterFilters = databaseWriterFilters;
    }
    
    public void setResolvedData(ResolvedData... resolvedData) {
        this.resolvedData = new ArrayList<ResolvedData>();
        if (resolvedData != null) {
            for (ResolvedData data : resolvedData) {
                this.resolvedData.add(data);
            }
        }
    }
    
    public void setResolvedData(List<ResolvedData> resolvedData) {
        this.resolvedData = resolvedData;
    }
    
    public List<ResolvedData> getResolvedData() {
        return resolvedData;
    }
    
    public void setDatabaseWriterErrorHandlers(
            List<IDatabaseWriterErrorHandler> databaseWriterErrorHandlers) {
        this.databaseWriterErrorHandlers = databaseWriterErrorHandlers;
    }
    
    public List<IDatabaseWriterErrorHandler> getDatabaseWriterErrorHandlers() {
        return databaseWriterErrorHandlers;
    }
    
    public ResolvedData getResolvedData (long rowNumber) {
        if (resolvedData != null) {
            for (ResolvedData data : resolvedData) {
                if (data.getRowNumber() == rowNumber) {
                    return data;
                }
            }
        }
        return null;
    }
    
    public void setIgnoreMissingTables(boolean ignoreMissingTables) {
        this.ignoreMissingTables = ignoreMissingTables;
    }
    
    public boolean isIgnoreMissingTables() {
        return ignoreMissingTables;
    }
    
    public void addErrorHandler(IDatabaseWriterErrorHandler handler) {
        if (this.databaseWriterErrorHandlers == null) {
            this.databaseWriterErrorHandlers = new ArrayList<IDatabaseWriterErrorHandler>();
        }
        this.databaseWriterErrorHandlers.add(handler);
    }

    public Conflict pickConflict(Table table, Batch batch) {
        Conflict settings = null;
        String fullyQualifiedName = table.getFullyQualifiedTableName();
        if (conflictSettingsByTable != null) {
            Conflict found = conflictSettingsByTable.get(fullyQualifiedName);

            if (found == null) {
                found = conflictSettingsByTable.get(table.getName());
            }

            if (found != null
                    && (StringUtils.isBlank(found.getTargetChannelId()) || found
                            .getTargetChannelId().equals(batch.getChannelId()))) {
                settings = found;
            }
        }

        if (settings == null && conflictSettingsByChannel != null) {
            settings = conflictSettingsByChannel.get(batch.getChannelId());
        }

        if (settings == null) {
            settings = defaultConflictSetting;
        }

        if (settings == null) {
            settings = new Conflict();
        }

        return settings;

    }

}
