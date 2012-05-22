package org.jumpmind.symmetric.load;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.DefaultTransformWriterConflictResolver;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDataLoaderFactory implements IDataLoaderFactory {

    private static final Logger log = LoggerFactory.getLogger(DefaultDataLoaderFactory.class);

    private IParameterService parameterService;

    public DefaultDataLoaderFactory(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public String getTypeName() {
        return "default";
    }

    public IDataWriter getDataWriter(final String sourceNodeId, final ISymmetricDialect symmetricDialect,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        DatabaseWriter writer = new DatabaseWriter(symmetricDialect.getPlatform(),
                new DefaultTransformWriterConflictResolver(transformWriter) {
                    @Override
                    protected void beforeResolutionAttempt() {
                        DatabaseWriter writer = (DatabaseWriter) transformWriter.getTargetWriter();
                        ISqlTransaction transaction = writer.getTransaction();
                        if (transaction != null) {
                            symmetricDialect.enableSyncTriggers(transaction);
                        }
                    }

                    @Override
                    protected void afterResolutionAttempt() {
                        // We cannot re-disable sync triggers because subsequent updates to the 
                        // same row that was in conflict will probably not be in conflict, but they
                        // should be "pinged" back to the updating 
                        // DatabaseWriter writer = (DatabaseWriter)
                        // transformWriter.getTargetWriter();
                        // ISqlTransaction transaction =
                        // writer.getTransaction();
                        // if (transaction != null) {
                        // symmetricDialect.disableSyncTriggers(transaction,
                        // sourceNodeId);
                        // }
                    }
                },
                buildDatabaseWriterSettings(filters, conflictSettings, resolvedData));
        return writer;
    }    

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

    protected DatabaseWriterSettings buildDatabaseWriterSettings(
            List<IDatabaseWriterFilter> filters, List<? extends Conflict> conflictSettings,
            List<ResolvedData> resolvedDatas) {
        DatabaseWriterSettings settings = new DatabaseWriterSettings();
        settings.setDatabaseWriterFilters(filters);
        settings.setMaxRowsBeforeCommit(parameterService
                .getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT));
        settings.setTreatDateTimeFieldsAsVarchar(parameterService
                .is(ParameterConstants.DATA_LOADER_TREAT_DATETIME_AS_VARCHAR));

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
            log.warn(
                    "There were multiple default conflict settings found.  Using '{}' as the default",
                    settings.getDefaultConflictSetting().getConflictId());
        }
        settings.setConflictSettingsByChannel(byChannel);
        settings.setConflictSettingsByTable(byTable);
        settings.setResolvedData(resolvedDatas);
        return settings;
    }

}
