package org.jumpmind.symmetric.load;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.ConflictSettings;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.DefaultTransformWriterConflictResolver;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.service.IParameterService;

public class DefaultDataLoaderFactory implements IDataLoaderFactory {

    private IParameterService parameterService;

    public DefaultDataLoaderFactory(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public String getTypeName() {
        return "default";
    }

    public IDataWriter getDataWriter(String sourceNodeId, IDatabasePlatform platform,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<? extends ConflictSettings> conflictSettings) {
        DatabaseWriter writer = new DatabaseWriter(platform,
                new DefaultTransformWriterConflictResolver(transformWriter),
                buildDatabaseWriterSettings(filters, conflictSettings));
        return writer;
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

    protected DatabaseWriterSettings buildDatabaseWriterSettings(
            List<IDatabaseWriterFilter> filters, List<? extends ConflictSettings> conflictSettings) {
        DatabaseWriterSettings settings = new DatabaseWriterSettings();
        settings.setDatabaseWriterFilters(filters);
        settings.setMaxRowsBeforeCommit(parameterService
                .getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT));
        settings.setTreatDateTimeFieldsAsVarchar(parameterService
                .is(ParameterConstants.DATA_LOADER_TREAT_DATETIME_AS_VARCHAR));

        Map<String, ConflictSettings> byChannel = new HashMap<String, ConflictSettings>();
        Map<String, ConflictSettings> byTable = new HashMap<String, ConflictSettings>();
        if (conflictSettings != null) {
            for (ConflictSettings conflictSetting : conflictSettings) {
                String qualifiedTableName = conflictSetting.toQualifiedTableName();
                if (StringUtils.isNotBlank(qualifiedTableName)) {
                    byTable.put(qualifiedTableName, conflictSetting);
                } else if (StringUtils.isNotBlank(conflictSetting.getTargetChannelId())) {
                    byChannel.put(conflictSetting.getTargetChannelId(), conflictSetting);
                } else {
                    settings.setDefaultConflictSetting(conflictSetting);
                }
            }
        }
        settings.setConflictSettingsByChannel(byChannel);
        settings.setConflictSettingsByTable(byTable);
        return settings;
    }

}
