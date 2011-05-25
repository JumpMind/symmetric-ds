package org.jumpmind.symmetric.core.process.sql;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.junit.Before;
import org.junit.Test;

public class SqlDataWriterTest extends AbstractDatabaseTest {

    Table testTable;

    @Before
    public void cleanupTestTable() {
        testTable = buildTestTable();
        delete(testTable.getTableName());
    }

    @Test
    public void testOneRowInsert() {
        writeToTestTable(new Data(testTable.getTableName(), DataEventType.INSERT, "1,\"test\""));
        Assert.assertEquals(1, count(testTable.getTableName()));
    }

    @Test
    public void testOneRowInsertOneRowUpdate() {
        writeToTestTable(new Data(testTable.getTableName(), DataEventType.INSERT, "1,\"test\""),
                new Data(testTable.getTableName(), DataEventType.UPDATE, "1", "1,\"updated\""));
        Assert.assertEquals(1, count(testTable.getTableName()));
        Assert.assertEquals(
                "updated",
                getPlatform().getSqlConnection().queryForObject(
                        String.format("select TEST_TEXT from %s where TEST_ID=?",
                                testTable.getTableName()), String.class, 1));
    }

    @Test
    public void testOneRowInsertOneRowDelete() {
        writeToTestTable(new Data(testTable.getTableName(), DataEventType.INSERT, "1,\"test\""),
                new Data(testTable.getTableName(), DataEventType.DELETE, "1", null));
        Assert.assertEquals(0, count(testTable.getTableName()));
    }

    @Test
    public void testOneRowInsertFallbackToUpdate() {
        insertTestTableRows(10);
        Assert.assertEquals(10, count(testTable.getTableName()));
        int existingId = getPlatform().getSqlConnection().queryForInt(
                String.format("select min(TEST_ID) from %s", testTable.getTableName()));
        Assert.assertEquals(1,
                count(testTable.getTableName(), String.format("TEST_ID=%d", existingId)));
        Assert.assertEquals(
                0,
                count(testTable.getTableName(),
                        String.format("TEST_ID=%d and TEST_TEXT='new value'", existingId)));
        Batch batch = writeToTestTable(new Data(testTable.getTableName(), DataEventType.INSERT,
                String.format("%d,\"new value\"", existingId)));
        Assert.assertEquals(10, count(testTable.getTableName()));
        Assert.assertEquals(1, batch.getFallbackUpdateCount());
        Assert.assertEquals(
                1,
                count(testTable.getTableName(),
                        String.format("TEST_ID=%d and TEST_TEXT='new value'", existingId)));
        Assert.assertEquals(0, batch.getInsertCount());
    }

    @Test
    public void testOneRowUpdateFallbackToInsert() {
        Assert.assertEquals(0, count(testTable.getTableName()));
        Batch batch = writeToTestTable(new Data(testTable.getTableName(), DataEventType.UPDATE,
                "1", "1,\"updated\""));
        Assert.assertEquals(1, count(testTable.getTableName()));
        Assert.assertEquals(1, batch.getFallbackInsertCount());
        Assert.assertEquals(0, batch.getUpdateCount());
    }

    @Test
    public void testOneRowUpdateFallbackUpdateWithNewKeys() {
        insertTestTableRows(1);
        Assert.assertEquals(1, count(testTable.getTableName()));
        int existingId = getPlatform().getSqlConnection().queryForInt(
                String.format("select min(TEST_ID) from %s", testTable.getTableName()));
        Batch batch = writeToTestTable(new Data(testTable.getTableName(), DataEventType.UPDATE,
                String.format("%d", existingId + 1), String.format("%d,\"updated\"", existingId)));
        Assert.assertEquals(1, count(testTable.getTableName()));
        Assert.assertEquals(0, batch.getFallbackInsertCount());
        Assert.assertEquals(1, batch.getFallbackUpdateWithNewKeysCount());
        Assert.assertEquals(0, batch.getUpdateCount());
        Assert.assertEquals(1,
                count(testTable.getTableName(), String.format("TEST_TEXT='updated'")));
    }
    
    @Test
    public void testSqlData() {
        insertTestTableRows(10);
        Assert.assertEquals(10, count(testTable.getTableName()));
        Batch batch = writeToTestTable(new Data(testTable.getTableName(), DataEventType.SQL, 
                String.format("\"update %s set TEST_TEXT='it worked!'\"", testTable.getTableName())));
        Assert.assertEquals(10, count(testTable.getTableName()));
        Assert.assertEquals(10, count(testTable.getTableName(), "TEST_TEXT='it worked!'"));
        Assert.assertEquals(1, batch.getSqlCount());
        Assert.assertEquals(10, batch.getSqlRowsAffectedCount());

    }

    protected Batch writeToTestTable(Data... datas) {
        SqlDataWriter writer = new SqlDataWriter(getPlatform(), new Parameters());
        DataContext ctx = new DataContext();
        writer.open(ctx);
        Batch batch = new Batch();
        writer.startBatch(batch);
        writer.writeTable(testTable);
        for (Data data : datas) {
            writer.writeData(data);
        }
        writer.finishBatch(batch);
        writer.close();
        return batch;
    }
}
