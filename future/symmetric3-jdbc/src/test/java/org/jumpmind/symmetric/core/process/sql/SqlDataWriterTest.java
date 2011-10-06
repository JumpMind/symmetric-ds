package org.jumpmind.symmetric.core.process.sql;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.db.IDbDialect;
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
        writeToTestTable(true, new Data(testTable.getTableName(), DataEventType.INSERT,
                "1,\"test\""));
        Assert.assertEquals(1, count(testTable.getTableName()));
    }

    @Test
    public void testOneRowInsertOneRowUpdate() {
        writeToTestTable(true, new Data(testTable.getTableName(), DataEventType.INSERT,
                "1,\"test\""), new Data(testTable.getTableName(), DataEventType.UPDATE, "1",
                "1,\"updated\""));
        Assert.assertEquals(1, count(testTable.getTableName()));
        IDbDialect dbDialect = getDbDialect(false);
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        Assert.assertEquals(
                "updated",
                getDbDialect().getSqlTemplate().queryForObject(
                        String.format("select TEST_TEXT from %s%s%s where TEST_ID=?", quoteString,
                                testTable.getTableName(), quoteString), String.class, 1));
    }

    @Test
    public void testOneRowInsertOneRowDelete() {
        writeToTestTable(true, new Data(testTable.getTableName(), DataEventType.INSERT,
                "1,\"test\""), new Data(testTable.getTableName(), DataEventType.DELETE, "1", null));
        Assert.assertEquals(0, count(testTable.getTableName()));
    }

    @Test
    public void testOneRowInsertFallbackToUpdate() {
        IDbDialect dbDialect = getDbDialect(false);
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        insertTestTableRows(10);
        Assert.assertEquals(10, count(testTable.getTableName()));
        int existingId = getDbDialect().getSqlTemplate().queryForInt(
                String.format("select min(TEST_ID) from %s%s%s", quoteString,
                        testTable.getTableName(), quoteString));
        Assert.assertEquals(1,
                count(testTable.getTableName(), String.format("TEST_ID=%d", existingId)));
        Assert.assertEquals(
                0,
                count(testTable.getTableName(),
                        String.format("TEST_ID=%d and TEST_TEXT='new value'", existingId)));
        Batch batch = writeToTestTable(true, new Data(testTable.getTableName(),
                DataEventType.INSERT, String.format("%d,\"new value\"", existingId)));
        Assert.assertEquals(10, count(testTable.getTableName()));
        Assert.assertEquals(1, batch.getFallbackUpdateCount());
        Assert.assertEquals(
                1,
                count(testTable.getTableName(),
                        String.format("TEST_ID=%d and TEST_TEXT='new value'", existingId)));
        Assert.assertEquals(0, batch.getInsertCount());
    }

    @Test
    public void testMultipleRowInsertFallbackToUpdateInBatchMode() {
        insertTestTableRows(10);
        Assert.assertEquals(10, count(testTable.getTableName()));
        IDbDialect dbDialect = getDbDialect(false);
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        int existingId1 = getDbDialect().getSqlTemplate().queryForInt(
                String.format("select min(TEST_ID) from %s%s%s", quoteString,
                        testTable.getTableName(), quoteString));
        int existingId2 = getDbDialect().getSqlTemplate().queryForInt(
                String.format("select max(TEST_ID) from %s%s%s", quoteString,
                        testTable.getTableName(), quoteString));
        int newId = existingId2 + 1000;
        Batch batch = writeToTestTable(
                true,
                new Data(testTable.getTableName(), DataEventType.INSERT, String.format(
                        "%d,\"new value\"", existingId1)),
                new Data(testTable.getTableName(), DataEventType.INSERT, String.format(
                        "%d,\"new value\"", existingId2)), new Data(testTable.getTableName(),
                        DataEventType.INSERT, String.format("%d,\"new value\"", newId)));
        Assert.assertEquals(11, count(testTable.getTableName()));
        Assert.assertEquals(
                1,
                count(testTable.getTableName(),
                        String.format("TEST_ID=%d and TEST_TEXT='new value'", newId)));
        Assert.assertEquals(2, batch.getFallbackUpdateCount());
        Assert.assertEquals(1, batch.getInsertCount());
    }

    @Test
    public void testMultipleRowInsertFallbackToUpdateOutOfBatchMode() {
        insertTestTableRows(10);
        Assert.assertEquals(10, count(testTable.getTableName()));
        IDbDialect dbDialect = getDbDialect(false);
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        int existingId1 = getDbDialect().getSqlTemplate().queryForInt(
                String.format("select min(TEST_ID) from %s%s%s", quoteString,
                        testTable.getTableName(), quoteString));
        int existingId2 = getDbDialect().getSqlTemplate().queryForInt(
                String.format("select max(TEST_ID) from %s%s%s", quoteString,
                        testTable.getTableName(), quoteString));
        int newId = existingId2 + 1000;
        Batch batch = writeToTestTable(
                false,
                new Data(testTable.getTableName(), DataEventType.INSERT, String.format(
                        "%d,\"new value\"", existingId1)),
                new Data(testTable.getTableName(), DataEventType.INSERT, String.format(
                        "%d,\"new value\"", existingId2)), new Data(testTable.getTableName(),
                        DataEventType.INSERT, String.format("%d,\"new value\"", newId)));
        Assert.assertEquals(11, count(testTable.getTableName()));
        Assert.assertEquals(
                1,
                count(testTable.getTableName(),
                        String.format("TEST_ID=%d and TEST_TEXT='new value'", newId)));
        Assert.assertEquals(2, batch.getFallbackUpdateCount());
        Assert.assertEquals(1, batch.getInsertCount());
    }

    @Test
    public void testOneRowUpdateFallbackToInsert() {
        Assert.assertEquals(0, count(testTable.getTableName()));
        Batch batch = writeToTestTable(true, new Data(testTable.getTableName(),
                DataEventType.UPDATE, "1", "1,\"updated\""));
        Assert.assertEquals(1, count(testTable.getTableName()));
        Assert.assertEquals(1, batch.getFallbackInsertCount());
        Assert.assertEquals(0, batch.getUpdateCount());
    }

    @Test
    public void testOneRowUpdateFallbackUpdateWithNewKeys() {
        insertTestTableRows(1);
        Assert.assertEquals(1, count(testTable.getTableName()));
        IDbDialect dbDialect = getDbDialect(false);
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        int existingId = getDbDialect().getSqlTemplate().queryForInt(
                String.format("select min(TEST_ID) from %s%s%s", quoteString,
                        testTable.getTableName(), quoteString));

        Batch batch = writeToTestTable(
                true,
                new Data(testTable.getTableName(), DataEventType.UPDATE, String.format("%d",
                        existingId + 1), String.format("%d,\"updated\"", existingId)));
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
        IDbDialect dbDialect = getDbDialect(false);
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        Batch batch = writeToTestTable(
                true,
                new Data(testTable.getTableName(), DataEventType.SQL, String.format(
                        "\"update \\%s%s\\%s set TEST_TEXT='it worked!'\"", quoteString,
                        testTable.getTableName(), quoteString)));
        Assert.assertEquals(10, count(testTable.getTableName()));
        Assert.assertEquals(10, count(testTable.getTableName(), "TEST_TEXT='it worked!'"));
        Assert.assertEquals(1, batch.getSqlCount());
        Assert.assertEquals(10, batch.getSqlRowsAffectedCount());

    }

    protected Batch writeToTestTable(boolean turnOnBatchMode, Data... datas) {
        Parameters params = new Parameters();
        params.put(Parameters.LOADER_USE_BATCHING, new Boolean(turnOnBatchMode).toString());
        SqlDataWriter writer = new SqlDataWriter(getDbDialect(), params);
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
