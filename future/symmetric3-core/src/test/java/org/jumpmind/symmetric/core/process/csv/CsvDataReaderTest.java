package org.jumpmind.symmetric.core.process.csv;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.csv.CsvConstants;
import org.junit.Test;

public class CsvDataReaderTest {

    @Test
    public void testSimpleRead() {
        String nodeId= "055";
        long batchId = 123;
        String channelId = "nbc";
        StringBuilder builder = beginCsv(nodeId);
        beginBatch(builder, batchId, channelId);
        putTableN(builder, 1, true);
        putInsert(builder, 4);
        endCsv(builder);
        
        CsvDataReader reader = new CsvDataReader(builder);
        DataContext ctx = new DataContext();
        reader.open(ctx);
        
        Batch batch = reader.nextBatch();
        Assert.assertNotNull(batch);
        Assert.assertEquals(batchId, batch.getBatchId());
        
        Table table = reader.nextTable();
        Assert.assertNotNull(table);
        Assert.assertEquals("test1", table.getTableName());
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumns().size());
        Assert.assertEquals("id", table.getColumn(0).getName());
        Assert.assertEquals("text", table.getColumn(1).getName());
        
        Data data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(channelId, data.getChannelId());
        Assert.assertEquals(DataEventType.INSERT, data.getEventType());
        Assert.assertEquals("0", data.toParsedRowData()[0]);
        Assert.assertEquals("test", data.toParsedRowData()[1]);
        
        data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(channelId, data.getChannelId());
        Assert.assertEquals(DataEventType.INSERT, data.getEventType());
        Assert.assertEquals("1", data.toParsedRowData()[0]);
        Assert.assertEquals("test", data.toParsedRowData()[1]);
        
        data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(channelId, data.getChannelId());
        Assert.assertEquals(DataEventType.INSERT, data.getEventType());
        Assert.assertEquals("2", data.toParsedRowData()[0]);
        Assert.assertEquals("test", data.toParsedRowData()[1]);
        
        data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(channelId, data.getChannelId());
        Assert.assertEquals(DataEventType.INSERT, data.getEventType());
        Assert.assertEquals("3", data.toParsedRowData()[0]);
        Assert.assertEquals("test", data.toParsedRowData()[1]);
        
        data = reader.nextData();
        Assert.assertNull(data);
        
        table = reader.nextTable();
        Assert.assertNull(table);
        
        batch = reader.nextBatch();
        Assert.assertNull(batch);
        
        reader.close();
    }

    protected StringBuilder beginCsv(String nodeId) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("%s,%s\n", CsvConstants.NODEID, nodeId));
        builder.append(String.format("%s,%s\n", CsvConstants.BINARY, BinaryEncoding.BASE64.toString()));
        return builder;
    }

    protected StringBuilder beginBatch(StringBuilder builder, long batchId, String channelId) {
        builder.append(String.format("%s,%s\n", CsvConstants.CHANNEL, channelId));
        builder.append(String.format("%s,%d\n", CsvConstants.BATCH, batchId));
        return builder;
    }

    protected StringBuilder putTableN(StringBuilder builder, int n, boolean includeMetadata) {
        builder.append(String.format("%s,%s\n", CsvConstants.SCHEMA, ""));
        builder.append(String.format("%s,%s\n", CsvConstants.CATALOG, ""));
        builder.append(String.format("%s,%s%d\n", CsvConstants.TABLE, "test", n));
        if (includeMetadata) {
            builder.append(String.format("%s,%s\n", CsvConstants.KEYS, "id"));
            builder.append(String.format("%s,%s,%s\n", CsvConstants.COLUMNS, "id", "text"));
        }
        return builder;
    }

    protected StringBuilder putInsert(StringBuilder builder, int count) {
        for (int i = 0; i < count; i++) {
            builder.append(String.format("%s,%d,%s\n", CsvConstants.INSERT, i, "\"test\""));
        }
        return builder;
    }

    protected StringBuilder endCsv(StringBuilder builder) {
        builder.append(String.format("%s\n", CsvConstants.COMMIT));
        return builder;
    }

}
