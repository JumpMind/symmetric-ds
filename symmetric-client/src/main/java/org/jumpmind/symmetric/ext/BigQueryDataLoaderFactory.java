package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.bigquery.BigQueryPlatform;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.BigQueryBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.AbstractDataLoaderFactory;
import org.jumpmind.symmetric.load.IDataLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.bigquery.BigQuery;

public class BigQueryDataLoaderFactory extends AbstractDataLoaderFactory implements IDataLoaderFactory {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    private IStagingManager stagingManager;
    
    private ISecurityService securityService;
    
    private BigQuery bigquery;
    
    public BigQueryDataLoaderFactory(ISymmetricEngine engine) {
        this.stagingManager = engine.getStagingManager();
        this.parameterService = engine.getParameterService();
        this.securityService = engine.getSecurityService();
        this.bigquery = ((BigQueryPlatform) engine.getSymmetricDialect().getTargetPlatform()).getBigQuery();
        
    }

    public String getTypeName() {
        return "bigquery_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
                TransformWriter transformWriter,
            List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
            List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {

        try {
            return new BigQueryBulkDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(), 
                    symmetricDialect.getTablePrefix(), stagingManager, filters, errorHandlers, parameterService, securityService, 
                    buildParameterDatabaseWritterSettings(), this.bigquery);
            
        } catch (Exception e) {
            log.warn(
                    "Failed to create the big query database writer.",
                    e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public boolean isPlatformSupported(IDatabasePlatform platform) {
        return (DatabaseNamesConstants.BIGQUERY.equals(platform.getName()));
    }

}
