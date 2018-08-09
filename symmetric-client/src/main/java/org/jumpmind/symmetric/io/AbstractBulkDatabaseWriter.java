package org.jumpmind.symmetric.io;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.IDataWriter;
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
    
    public final void write(CsvData data) {
        if (context.get(IDataWriter.CONTEXT_BULK_WRITER_TO_USE) != null && context.get(IDataWriter.CONTEXT_BULK_WRITER_TO_USE).equals("default")) {
            writeDefault(data);
        }else{
            context.put(IDataWriter.CONTEXT_BULK_WRITER_TO_USE, "bulk");
            bulkWrite(data);
        }
    }
    
    protected final void writeDefault(CsvData data) {
        super.write(data);
    }
    
    protected abstract void bulkWrite(CsvData data);

}
