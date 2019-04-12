package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.SnowflakeBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.AbstractDataLoaderFactory;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.jumpmind.symmetric.service.IParameterService;

public class SnowflakeBulkDataLoaderFactory extends AbstractDataLoaderFactory implements IDataLoaderFactory {
    
    private IStagingManager stagingManager;
    
    public SnowflakeBulkDataLoaderFactory(ISymmetricEngine engine) {
        this.stagingManager = engine.getStagingManager();
        this.parameterService = engine.getParameterService();
    }

    public String getTypeName() {
        return "snowflake_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
                TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {

        return new SnowflakeBulkDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(), 
                symmetricDialect.getTablePrefix(), stagingManager, filters, errorHandlers, parameterService, 
                buildParameterDatabaseWritterSettings());
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return (DatabaseNamesConstants.SNOWFLAKE.equals(platform.getName()));
    }

}
