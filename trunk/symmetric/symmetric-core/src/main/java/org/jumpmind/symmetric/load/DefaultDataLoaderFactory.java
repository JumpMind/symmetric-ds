package org.jumpmind.symmetric.load;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.IDataWriter;
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
            TransformWriter transformWriter, IDatabaseWriterFilter[] filters) {
        DatabaseWriterSettings defaultSettings = buildDatabaseWriterSettings();
        DatabaseWriter writer = new DatabaseWriter(platform, defaultSettings, null, filters);
        writer.setConflictResolver(new DefaultTransformWriterConflictResolver(transformWriter));
        return writer;
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return true;
    }

    protected DatabaseWriterSettings buildDatabaseWriterSettings() {
        DatabaseWriterSettings settings = new DatabaseWriterSettings();
        settings.setConflictResolutionDeletes(parameterService
                .is(ParameterConstants.DATA_LOADER_ALLOW_MISSING_DELETE) ? DatabaseWriterSettings.ConflictResolutionDeletes.IGNORE_CONTINUE
                : DatabaseWriterSettings.ConflictResolutionDeletes.ERROR_STOP);
        settings.setConflictResolutionInserts(parameterService
                .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_UPDATE) ? DatabaseWriterSettings.ConflictResolutionInserts.FALLBACK_UPDATE
                : DatabaseWriterSettings.ConflictResolutionInserts.ERROR_STOP);
        settings.setConflictResolutionUpdates(parameterService
                .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_INSERT) ? DatabaseWriterSettings.ConflictResolutionUpdates.FALLBACK_INSERT
                : DatabaseWriterSettings.ConflictResolutionUpdates.ERROR_STOP);
        settings.setMaxRowsBeforeCommit(parameterService
                .getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT));
        settings.setTreatDateTimeFieldsAsVarchar(parameterService
                .is(ParameterConstants.DATA_LOADER_TREAT_DATETIME_AS_VARCHAR));
        return settings;
    }

}
