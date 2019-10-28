package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcUtils;
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
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class SnowflakeBulkDataLoaderFactory implements IDataLoaderFactory {
    
    private NativeJdbcExtractor jdbcExtractor;
    private IStagingManager stagingManager;
    private IParameterService parameterService;

    public SnowflakeBulkDataLoaderFactory(ISymmetricEngine engine) {
        this.jdbcExtractor = JdbcUtils.getNativeJdbcExtractory();
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

        int maxRowsBeforeFlush = parameterService.getInt("snowflake.bulk.load.max.rows.before.flush", 100000);
        
        return new SnowflakeBulkDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(), 
                symmetricDialect.getTablePrefix(), stagingManager);
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return (DatabaseNamesConstants.SNOWFLAKE.equals(platform.getName()));
    }

}
