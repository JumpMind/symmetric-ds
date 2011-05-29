package org.jumpmind.symmetric.core.process.sql;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.csv.CsvConstants;
import org.junit.Test;

public class CsvDataReaderTest {

    @Test
    public void testSimpleRead() {
        
    }
    
    
    protected StringBuilder beginCsv(String nodeId) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("%s,%s", CsvConstants.NODEID, nodeId));
        builder.append(String.format("%s,%s", CsvConstants.BINARY, BinaryEncoding.BASE64.toString()));
        return builder;
    }
    
    protected StringBuilder beginBatch(StringBuilder builder, long batchId, String channelId) {
        builder.append(String.format("%s,%d", CsvConstants.CHANNEL, channelId));
        builder.append(String.format("%s,%d", CsvConstants.BATCH, batchId));
        return builder;
    }
    
    protected StringBuilder endCsv(StringBuilder builder) {
        builder.append(String.format("%s", CsvConstants.COMMIT));
        return builder;
    }
    
}
