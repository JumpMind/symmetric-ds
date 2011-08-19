package org.jumpmind.symmetric.core.process.transform;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.AbstractDataWriter;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IDataWriter;

public class TransformDataWriter extends AbstractDataWriter {

    private IDataWriter targetWriter;    
    
    public void startBatch(Batch batch) {

    }

    public boolean writeTable(Table table) {

        return false;
    }

    public boolean writeData(Data data) {

        return false;
    }

    public void finishBatch(Batch batch) {


    }

    public void open(DataContext context) {


    }

    public void close() {


    }

}
