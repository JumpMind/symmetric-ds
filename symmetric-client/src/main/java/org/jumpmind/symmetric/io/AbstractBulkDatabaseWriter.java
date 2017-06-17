package org.jumpmind.symmetric.io;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterSettings;
import org.jumpmind.symmetric.io.data.writer.DefaultDatabaseWriter;
import org.jumpmind.symmetric.model.IncomingBatch;

public abstract class AbstractBulkDatabaseWriter extends DefaultDatabaseWriter{
    
    protected boolean useDefaultDataWriter;
    
    public AbstractBulkDatabaseWriter(IDatabasePlatform platform){
        super(platform);
    }
    
    public AbstractBulkDatabaseWriter(IDatabasePlatform platform, DatabaseWriterSettings settings) {
        super(platform, settings);
    }
    
    public final void write(CsvData data) {
        if (useDefaultDataWriter) {
            writeDefault(data);
        }else{
            bulkWrite(data);
        }
    }
    
    protected final void writeDefault(CsvData data) {
        super.write(data);
    }
    
    protected abstract void bulkWrite(CsvData data);
    
    @Override
    public void start(Batch batch) {
        super.start(batch);
        IncomingBatch currentBatch = (IncomingBatch) context.get("currentBatch");
        useDefaultDataWriter = currentBatch == null ? false : currentBatch.isErrorFlag();
    }
}
