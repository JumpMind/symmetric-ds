package org.jumpmind.symmetric.ext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.load.DefaultDataLoaderFactory;
import org.jumpmind.symmetric.load.IDataLoaderFactory;

public class BulkDataLoaderFactory implements IDataLoaderFactory, ISymmetricEngineAware, IBuiltInExtensionPoint {

    ISymmetricEngine engine;
    Map<String, IDataLoaderFactory> dataLoaderFactories = new HashMap<String, IDataLoaderFactory>();

    @Override
    public String getTypeName() {
        return "bulk";
    }

    @Override
    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect, TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {

        for (IDataLoaderFactory factory : engine.getExtensionService().getExtensionPointList(IDataLoaderFactory.class)) {
            dataLoaderFactories.put(factory.getTypeName(), factory);
        }

        if (DatabaseNamesConstants.MYSQL.equals(engine.getSymmetricDialect().getTargetPlatform().getName())) {
            return new MySqlBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.MSSQL2000.equals(engine.getSymmetricDialect().getTargetPlatform().getName())
                || DatabaseNamesConstants.MSSQL2005.equals(engine.getSymmetricDialect().getTargetPlatform().getName())
                || DatabaseNamesConstants.MSSQL2008.equals(engine.getSymmetricDialect().getTargetPlatform().getName())) {
            return new MsSqlBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.ORACLE.equals(engine.getSymmetricDialect().getTargetPlatform().getName())) {
            return new OracleBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.POSTGRESQL.equals(engine.getSymmetricDialect().getTargetPlatform().getName())
                || DatabaseNamesConstants.GREENPLUM.equals(engine.getSymmetricDialect().getTargetPlatform().getName())) {
            return new PostgresBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (DatabaseNamesConstants.REDSHIFT.equals(engine.getSymmetricDialect().getTargetPlatform().getName())) {
            return new RedshiftBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else if (engine.getSymmetricDialect().getTargetPlatform().getName() != null &&
        		engine.getSymmetricDialect().getTargetPlatform().getName().startsWith(DatabaseNamesConstants.TERADATA)) {
            return new TeradataBulkDataLoaderFactory(engine).getDataWriter(sourceNodeId, symmetricDialect, transformWriter,
                    filters, errorHandlers, conflictSettings, resolvedData);
        } else {
            return dataLoaderFactories.get(new DefaultDataLoaderFactory().getTypeName()).getDataWriter(sourceNodeId,
                    symmetricDialect, transformWriter, filters, errorHandlers, conflictSettings, resolvedData);
        }
    }

    @Override
    public boolean isPlatformSupported(IDatabasePlatform platform) {
        if (DatabaseNamesConstants.MYSQL.equals(platform.getName())
                || DatabaseNamesConstants.MSSQL2000.equals(platform.getName())
                || DatabaseNamesConstants.MSSQL2005.equals(platform.getName())
                || DatabaseNamesConstants.MSSQL2008.equals(platform.getName())
                || DatabaseNamesConstants.ORACLE.equals(platform.getName())
                || DatabaseNamesConstants.POSTGRESQL.equals(platform.getName())
                || DatabaseNamesConstants.GREENPLUM.equals(platform.getName())
                || DatabaseNamesConstants.REDSHIFT.equals(platform.getName())
                	|| (platform.getName() != null && platform.getName().startsWith(DatabaseNamesConstants.TERADATA))) {
            return true;
        }
        return false;
    }

    @Override
    public void setSymmetricEngine(ISymmetricEngine engine) {
        this.engine = engine;
    }

}
