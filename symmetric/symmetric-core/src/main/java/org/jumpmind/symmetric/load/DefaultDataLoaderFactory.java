package org.jumpmind.symmetric.load;

import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.io.data.transform.TransformTable;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.TransformDatabaseWriter;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;

public class DefaultDataLoaderFactory implements IDataLoaderFactory {

    private IParameterService parameterService;

    private ITransformService transformService;

    private INodeService nodeService;

    private List<IDatabaseWriterFilter> filters;

    public DefaultDataLoaderFactory(IParameterService parameterService,
            ITransformService transformService, INodeService nodeService,
            List<IDatabaseWriterFilter> filters) {
        this.parameterService = parameterService;
        this.transformService = transformService;
        this.nodeService = nodeService;
        this.filters = filters;
    }

    public boolean isAutoRegister() {
        return true;
    }

    public String getTypeName() {
        return "default";
    }

    public IDataWriter getDataWriter(String sourceNodeId, IDatabasePlatform platform) {
        DatabaseWriterSettings settings = buildDatabaseWriterSettings();

        TransformTable[] transforms = null;
        if (sourceNodeId != null) {
            List<TransformTableNodeGroupLink> transformsList = transformService.findTransformsFor(
                    new NodeGroupLink(sourceNodeId, nodeService.findIdentityNodeId()),
                    TransformPoint.LOAD, true);
            transforms = transformsList != null ? transformsList
                    .toArray(new TransformTable[transformsList.size()]) : null;
        }

        return new TransformDatabaseWriter(platform, settings, null, transforms,
                filters.toArray(new IDatabaseWriterFilter[filters.size()]));
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
