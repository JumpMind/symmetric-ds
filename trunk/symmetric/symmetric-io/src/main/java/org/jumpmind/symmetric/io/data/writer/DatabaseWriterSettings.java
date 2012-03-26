package org.jumpmind.symmetric.io.data.writer;

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

    protected Map<String, Conflict> conflictSettingsByChannel;

    protected Map<String, Conflict> conflictSettingsByTable;

    protected List<IDatabaseWriterFilter> databaseWriterFilters;
    
    protected List<ResolvedData> resolvedData;

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
    
    public void setResolvedData(List<ResolvedData> resolvedData) {
        this.resolvedData = resolvedData;
    }
    
    public List<ResolvedData> getResolvedData() {
        return resolvedData;
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
