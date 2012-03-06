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

    protected ConflictSettings defaultConflictSetting;

    protected Map<String, ConflictSettings> conflictSettingsByChannel;

    protected Map<String, ConflictSettings> conflictSettingsByTable;

    protected List<IDatabaseWriterFilter> databaseWriterFilters;

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

    public ConflictSettings getDefaultConflictSetting() {
        return defaultConflictSetting;
    }

    public void setDefaultConflictSetting(ConflictSettings defaultConflictSetting) {
        this.defaultConflictSetting = defaultConflictSetting;
    }

    public Map<String, ConflictSettings> getConflictSettingsByChannel() {
        return conflictSettingsByChannel;
    }

    public void setConflictSettingsByChannel(Map<String, ConflictSettings> conflictSettingsByChannel) {
        this.conflictSettingsByChannel = conflictSettingsByChannel;
    }

    public Map<String, ConflictSettings> getConflictSettingsByTable() {
        return conflictSettingsByTable;
    }

    public void setConflictSettingsByTable(Map<String, ConflictSettings> conflictSettingsByTable) {
        this.conflictSettingsByTable = conflictSettingsByTable;
    }

    public List<IDatabaseWriterFilter> getDatabaseWriterFilters() {
        return databaseWriterFilters;
    }

    public void setDatabaseWriterFilters(List<IDatabaseWriterFilter> databaseWriterFilters) {
        this.databaseWriterFilters = databaseWriterFilters;
    }

    public ConflictSettings getConflictSettings(Table table, Batch batch) {
        ConflictSettings settings = null;
        String fullyQualifiedName = table.getFullyQualifiedTableName();
        if (conflictSettingsByTable != null) {
            ConflictSettings found = conflictSettingsByTable.get(fullyQualifiedName);

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
            settings = new ConflictSettings();
        }

        return settings;

    }

}
