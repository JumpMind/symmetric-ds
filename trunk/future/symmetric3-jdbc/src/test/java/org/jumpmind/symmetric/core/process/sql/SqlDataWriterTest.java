package org.jumpmind.symmetric.core.process.sql;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
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

    protected void writeToTestTable(Data... datas) {
        SqlDataWriter writer = new SqlDataWriter(getPlatform(), new Parameters());
        writer.open(writer.createDataContext());
        Batch batch = new Batch();
        writer.startBatch(batch);
        writer.switchTables(testTable);
        for (Data data : datas) {
            writer.writeData(data);
        }
        writer.finishBatch(batch);
        writer.close();
    }
}
