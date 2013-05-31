package org.jumpmind.symmetric.model;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.NestedDataWriter;

public class ProcessInfoDataWriter extends NestedDataWriter {

    private ProcessInfo processInfo;

    public ProcessInfoDataWriter(IDataWriter targetWriter, ProcessInfo processInfo) {
        super(targetWriter);
        this.processInfo = processInfo;
    }

    public void open(DataContext context) {
        super.open(context);
        processInfo.setDataCount(0);
        processInfo.setBatchCount(0);
    }

    public void start(Batch batch) {
        if (batch != null) {
            processInfo.setCurrentBatchId(batch.getBatchId());
            processInfo.setCurrentChannelId(batch.getChannelId());
            processInfo.incrementBatchCount();
        }
        super.start(batch);
    }

    public boolean start(Table table) {
        if (table != null) {
            processInfo.setCurrentTableName(table.getFullyQualifiedTableName());
        }
        return super.start(table);
    }

    public void write(CsvData data) {
        if (data != null) {
            processInfo.incrementDataCount();
        }
        super.write(data);        
    }

}
