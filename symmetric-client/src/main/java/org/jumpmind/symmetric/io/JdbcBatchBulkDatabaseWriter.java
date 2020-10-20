package org.jumpmind.symmetric.io;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;

public class JdbcBatchBulkDatabaseWriter extends AbstractBulkDatabaseWriter {

    private int lastRowCount = 0;
    
    private int expectedRowCount = 0;
    
    public JdbcBatchBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, 
            String tablePrefix, DatabaseWriterSettings writerSettings) {
        super(symmetricPlatform, targetPlatform, tablePrefix, writerSettings);
    }

    @Override
    public void start(Batch batch) {
        super.start(batch);
        if (context.get(ContextConstants.CONTEXT_BULK_WRITER_TO_USE) == null || !context.get(ContextConstants.CONTEXT_BULK_WRITER_TO_USE).equals("default")) {
            getTransaction().setInBatchMode(true);
            ((JdbcSqlTransaction) getTransaction()).setBatchSize(((JdbcSqlTemplate) getPlatform()
                    .getSqlTemplate()).getSettings().getBatchBulkLoaderSize());
        }
    }
    
    @Override
    protected void bulkWrite(CsvData data) {
        writeDefault(data);
    }
    
    @Override
    protected LoadStatus delete(CsvData data, boolean useConflictDetection) {
    	LoadStatus loadStatus = super.delete(data, useConflictDetection);
    	if (!getTransaction().isInBatchMode()) {
        	return loadStatus;
        }
    	checkForConflict(true);
    	return LoadStatus.SUCCESS;
    }
    
    @Override
    protected LoadStatus insert(CsvData data) {
    	LoadStatus loadStatus = super.insert(data);
    	if (!getTransaction().isInBatchMode()) {
        	return loadStatus;
        }
    	checkForConflict(true);
    	return LoadStatus.SUCCESS;
    }
    
    @Override
    protected LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection) {
        LoadStatus loadStatus = super.update(data, applyChangesOnly, useConflictDetection);
        if (!getTransaction().isInBatchMode()) {
        	return loadStatus;
        }
        checkForConflict(true);
    	return LoadStatus.SUCCESS;
    }
    
    protected void checkForConflict(boolean isDml) {
    	if (isDml) {
    		expectedRowCount++;
    	}
    	if (getTransaction().getUnflushedMarkers(false).size() == 0) {
    		if (expectedRowCount != lastRowCount) {
    			throw new SymmetricException("JdbcBatchBulkDataWriter was in conflict, will attempt to fallback using default writer.");
    		}
    		expectedRowCount=0;
    		lastRowCount=0;
        }
    }
    
    @Override
    protected void prepare() {
    	if (getTransaction().isInBatchMode()) {
    		lastRowCount = getTransaction().flush();
    		checkForConflict(false);
    	}
    	super.prepare();
    }
    
    protected int execute(CsvData data, String[] values) {
    	lastRowCount = super.execute(data, values);
    	return lastRowCount;
    }
    
    @Override
    public void end(Batch batch, boolean inError) {
    	if (getTransaction().isInBatchMode()) {
    		lastRowCount = getTransaction().flush();
    		checkForConflict(false);
    	}
    	super.end(batch, inError);
    }
    
    
}

