package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.MsSqlBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class MsSqlBulkDataLoaderFactory implements IDataLoaderFactory,
		ISymmetricEngineAware, IBuiltInExtensionPoint {

    private int maxRowsBeforeFlush;
    private NativeJdbcExtractor jdbcExtractor;
    private IStagingManager stagingManager;

    public MsSqlBulkDataLoaderFactory() {
        this.jdbcExtractor = JdbcUtils.getNativeJdbcExtractory();
    }

    public String getTypeName() {
        return "mssql_bulk";
    }

	public IDataWriter getDataWriter(String sourceNodeId,
			ISymmetricDialect symmetricDialect,
			TransformWriter transformWriter,
			List<IDatabaseWriterFilter> filters,
			List<IDatabaseWriterErrorHandler> errorHandlers,
			List<? extends Conflict> conflictSettings,
			List<ResolvedData> resolvedData) {
		return new MsSqlBulkDatabaseWriter(symmetricDialect.getPlatform(),
				stagingManager, jdbcExtractor, maxRowsBeforeFlush);
	}

    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.maxRowsBeforeFlush = engine.getParameterService().getInt(
                "mssql.bulk.load.max.rows.before.flush", 100000);
        //TODO: pass information about the destination database such that we can do the 
        //TODO: bulk load to the remote server vs using the T-SQL  BULK INSERT statement
        this.stagingManager = engine.getStagingManager();
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return DatabaseNamesConstants.MSSQL.equals(platform.getName());
    }

}
