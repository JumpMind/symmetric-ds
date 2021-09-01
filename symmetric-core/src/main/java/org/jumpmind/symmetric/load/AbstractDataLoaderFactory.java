package org.jumpmind.symmetric.load;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDataLoaderFactory {
	
    protected IParameterService parameterService;
    
    protected final Logger log = LoggerFactory.getLogger(getClass());

    public DatabaseWriterSettings buildParameterDatabaseWriterSettings(List<? extends Conflict> conflictSettings) {
        DatabaseWriterSettings settings = new DatabaseWriterSettings();
        
        settings.setCreateTableAlterCaseToMatchDatabaseDefault(
                parameterService.is(ParameterConstants.DATA_LOADER_CREATE_TABLE_ALTER_TO_MATCH_DB_CASE, true));
        settings.setMaxRowsBeforeCommit(
                parameterService.getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT));
        settings.setCommitSleepInterval(
                parameterService.getLong(ParameterConstants.DATA_LOADER_SLEEP_TIME_AFTER_EARLY_COMMIT));
        settings.setIgnoreMissingTables(parameterService.is(ParameterConstants.DATA_LOADER_IGNORE_MISSING_TABLES));
        settings.setTreatDateTimeFieldsAsVarchar(
                parameterService.is(ParameterConstants.DATA_LOADER_TREAT_DATETIME_AS_VARCHAR));
        settings.setSaveCurrentValueOnError(
                parameterService.is(ParameterConstants.DATA_LOADER_ERROR_RECORD_CUR_VAL, false));
        settings.setFitToColumn(parameterService.is(ParameterConstants.DATA_LOADER_FIT_TO_COLUMN, false));
        settings.setLogConflictResolution(parameterService.is(ParameterConstants.LOG_CONFLICT_RESOLUTION));
        settings.setTextColumnExpression(
                parameterService.getString(ParameterConstants.DATA_LOADER_TEXT_COLUMN_EXPRESSION));
        settings.setApplyChangesOnly(parameterService.is(ParameterConstants.DATA_LOADER_APPLY_CHANGES_ONLY, true));
        settings.setUsePrimaryKeysFromSource(
                parameterService.is(ParameterConstants.DATA_LOADER_USE_PRIMARY_KEYS_FROM_SOURCE));
        
        Map<String, Conflict> byChannel = new HashMap<String, Conflict>();
        Map<String, Conflict> byTable = new HashMap<String, Conflict>();
        boolean multipleDefaultSettingsFound = false;
        if (conflictSettings != null) {
            for (Conflict conflictSetting : conflictSettings) {
                String qualifiedTableName = conflictSetting.toQualifiedTableName();
                if (StringUtils.isNotBlank(qualifiedTableName)) {
                    byTable.put(qualifiedTableName, conflictSetting);
                } else if (StringUtils.isNotBlank(conflictSetting.getTargetChannelId())) {
                    byChannel.put(conflictSetting.getTargetChannelId(), conflictSetting);
                } else {
                    if (settings.getDefaultConflictSetting() != null) {
                        multipleDefaultSettingsFound = true;
                    }
                    settings.setDefaultConflictSetting(conflictSetting);
                }
            }
        }

        if (multipleDefaultSettingsFound) {
            log.warn("There were multiple default conflict settings found.  Using '{}' as the default",
                    settings.getDefaultConflictSetting().getConflictId());
        }
        settings.setConflictSettingsByChannel(byChannel);
        settings.setConflictSettingsByTable(byTable);
        
        return settings;
    }
}
