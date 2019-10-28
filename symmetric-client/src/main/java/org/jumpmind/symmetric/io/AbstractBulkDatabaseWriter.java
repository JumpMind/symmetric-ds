package org.jumpmind.symmetric.io;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.DynamicDefaultDatabaseWriter;

public abstract class AbstractBulkDatabaseWriter extends DynamicDefaultDatabaseWriter{
    
    public AbstractBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, String tablePrefix){
        super(symmetricPlatform, targetPlatform, tablePrefix);
    }
    
    public AbstractBulkDatabaseWriter(IDatabasePlatform symmetricPlatform, IDatabasePlatform targetPlatform, 
    		String tablePrefix, DatabaseWriterSettings settings) {
        super(symmetricPlatform, targetPlatform, tablePrefix, settings);
    }
    
    @Override
    public void start(Batch batch) {
        super.start(batch);
        if (isFallBackToDefault()) {
            getTransaction().setInBatchMode(false);
            log.debug("Writing batch " + batch.getBatchId() + " on channel " + batch.getChannelId() + " to node " + batch.getTargetNodeId() + " using DEFAULT loader");
        }else{
            log.debug("Writing batch " + batch.getBatchId() + " on channel " + batch.getChannelId() + " to node " + batch.getTargetNodeId() + " using BULK loader");
        }
    }
    
    public final void write(CsvData data) {
        if (isFallBackToDefault()) {
            writeDefault(data);
        }else{
            context.put(ContextConstants.CONTEXT_BULK_WRITER_TO_USE, "bulk");
            bulkWrite(data);
        }
    }
    
    @Override
    public void end(Batch batch, boolean inError) {
        super.end(batch, inError);
        if (!inError) {
            context.put(ContextConstants.CONTEXT_BULK_WRITER_TO_USE, null);
        }
    }
    
    public boolean isFallBackToDefault() {
        return context.get(ContextConstants.CONTEXT_BULK_WRITER_TO_USE) != null && context.get(ContextConstants.CONTEXT_BULK_WRITER_TO_USE).equals("default");
    }
    
    protected final void writeDefault(CsvData data) {
        super.write(data);
    }
    
    protected abstract void bulkWrite(CsvData data);

}
