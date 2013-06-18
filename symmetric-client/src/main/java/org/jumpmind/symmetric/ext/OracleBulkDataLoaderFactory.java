package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcUtils;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.OracleBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

public class OracleBulkDataLoaderFactory implements IDataLoaderFactory, ISymmetricEngineAware,
        IBuiltInExtensionPoint {

    private String procedurePrefix;
    private int maxRowsBeforeFlush;
    private NativeJdbcExtractor jdbcExtractor;

    public OracleBulkDataLoaderFactory() {
        this.jdbcExtractor = JdbcUtils.getNativeJdbcExtractory();
    }

    public String getTypeName() {
        return "oracle_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
            TransformWriter transformWriter, List<IDatabaseWriterFilter> filters,
            List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {
        return new OracleBulkDatabaseWriter(symmetricDialect.getPlatform(), procedurePrefix,
                jdbcExtractor, maxRowsBeforeFlush);
    }

    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.procedurePrefix = engine.getTablePrefix();
        this.maxRowsBeforeFlush = engine.getParameterService().getInt(
                "oracle.bulk.load.max.rows.before.flush", 1000);
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return DatabaseNamesConstants.ORACLE.equals(platform.getName());
    }

}
