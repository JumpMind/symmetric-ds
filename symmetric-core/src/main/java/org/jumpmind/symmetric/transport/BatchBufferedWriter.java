package org.jumpmind.symmetric.transport;

import java.io.BufferedWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class BatchBufferedWriter extends BufferedWriter {
    
    List<Long> batchIds = new ArrayList<Long>();
    
    public BatchBufferedWriter(Writer out) {
        super(out);
    }
    
    public List<Long> getBatchIds() {
        return batchIds;
    }
}