package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.TeradataBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterErrorHandler;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.ResolvedData;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.load.IDataLoaderFactory;

public class TeradataBulkDataLoaderFactory implements IDataLoaderFactory {
	private IStagingManager stagingManager;
    
    public TeradataBulkDataLoaderFactory(ISymmetricEngine engine) {
        this.stagingManager = engine.getStagingManager();
    }

    public String getTypeName() {
        return "teradata_bulk";
    }

    public IDataWriter getDataWriter(String sourceNodeId, ISymmetricDialect symmetricDialect,
			TransformWriter transformWriter,
        List<IDatabaseWriterFilter> filters, List<IDatabaseWriterErrorHandler> errorHandlers,
        List<? extends Conflict> conflictSettings, List<ResolvedData> resolvedData) {

	    return new TeradataBulkDatabaseWriter(symmetricDialect.getPlatform(), symmetricDialect.getTargetPlatform(), symmetricDialect.getTablePrefix(), 
	    		stagingManager);
	}
	
	public boolean isPlatformSupported(IDatabasePlatform platform) {
	    return (platform.getName() != null && platform.getName().startsWith(DatabaseNamesConstants.TERADATA));
	}

}
