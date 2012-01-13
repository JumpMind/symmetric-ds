package org.jumpmind.symmetric.io.data.reader;

import junit.framework.Assert;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataWriter;
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
        
        ProtocolDataReader reader = new ProtocolDataReader(builder);
        DataContext<ProtocolDataReader, IDataWriter> ctx = new DataContext<ProtocolDataReader, IDataWriter>(reader, null);
        reader.open(ctx);
        
        Batch batch = reader.nextBatch();
        Assert.assertNotNull(batch);
        Assert.assertEquals(batchId, batch.getBatchId());
        
        Table table = reader.nextTable();
        Assert.assertNotNull(table);
        Assert.assertEquals("test1", table.getName());
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumns().length);
        Assert.assertEquals("id", table.getColumn(0).getName());
        Assert.assertEquals("text", table.getColumn(1).getName());
        
        CsvData data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(DataEventType.INSERT, data.getDataEventType());
        Assert.assertEquals("0", data.getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("test", data.getParsedData(CsvData.ROW_DATA)[1]);
        
        data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(DataEventType.INSERT, data.getDataEventType());
        Assert.assertEquals("1", data.getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("test", data.getParsedData(CsvData.ROW_DATA)[1]);
        
        data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(DataEventType.INSERT, data.getDataEventType());
        Assert.assertEquals("2", data.getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("test", data.getParsedData(CsvData.ROW_DATA)[1]);
        
        data = reader.nextData();
        Assert.assertNotNull(data);
        Assert.assertEquals(DataEventType.INSERT, data.getDataEventType());
        Assert.assertEquals("3", data.getParsedData(CsvData.ROW_DATA)[0]);
        Assert.assertEquals("test", data.getParsedData(CsvData.ROW_DATA)[1]);
        
        data = reader.nextData();
        Assert.assertNull(data);
        
        table = reader.nextTable();
        Assert.assertNull(table);
        
        batch = reader.nextBatch();
        Assert.assertNull(batch);
        
        reader.close();
    }
    
    @Test
    public void testTableContextSwitch() {
        String nodeId= "1";
        long batchId = 1;
        String channelId = "test";
        StringBuilder builder = beginCsv(nodeId);
        beginBatch(builder, batchId, channelId);
        putTableN(builder, 1, true);
        putInsert(builder, 4);
        putTableN(builder, 2, true);
        putInsert(builder, 4);        
        putTableN(builder, 1, false);
        putInsert(builder, 2);
        putTableN(builder, 2, false);
        putInsert(builder, 2);
        endCsv(builder);
        
        ProtocolDataReader reader = new ProtocolDataReader(builder);
        DataContext<ProtocolDataReader, IDataWriter> ctx = new DataContext<ProtocolDataReader, IDataWriter>(reader, null);
        reader.open(ctx);
        
        Batch batch = reader.nextBatch();
        Assert.assertNotNull(batch);
        
        Table table = reader.nextTable();
        Assert.assertNotNull(table);
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumnCount());
        Assert.assertEquals("test1", table.getName());
        
        int dataCount = 0;
        while (reader.nextData() != null) {
            dataCount++;
        }
        
        Assert.assertEquals(4, dataCount);
        
        table = reader.nextTable();
        Assert.assertNotNull(table);
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumnCount());
        Assert.assertEquals("test2", table.getName());
        
        dataCount = 0;
        while (reader.nextData() != null) {
            dataCount++;
        }
        
        Assert.assertEquals(4, dataCount);
        
        table = reader.nextTable();
        Assert.assertNotNull(table);
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumnCount());
        Assert.assertEquals("test1", table.getName());
        
        dataCount = 0;
        while (reader.nextData() != null) {
            dataCount++;
        }
        
        Assert.assertEquals(2, dataCount);
        
        table = reader.nextTable();
        Assert.assertNotNull(table);
        Assert.assertEquals(2, table.getColumnCount());
        Assert.assertEquals(1, table.getPrimaryKeyColumnCount());
        Assert.assertEquals("test2", table.getName());
        
        dataCount = 0;
        while (reader.nextData() != null) {
            dataCount++;
        }
        
        Assert.assertEquals(2, dataCount);
        

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
