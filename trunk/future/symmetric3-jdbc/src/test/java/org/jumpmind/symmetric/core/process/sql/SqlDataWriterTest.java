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
        SqlDataWriter writer = new SqlDataWriter(getPlatform(), new Parameters());
        writer.open(writer.createDataContext());
        Batch batch = new Batch();
        writer.startBatch(batch);
        writer.switchTables(testTable);
        writer.writeData(new Data(testTable.getTableName(), DataEventType.INSERT, "1,\"test\""));
        writer.finishBatch(batch);
        writer.close();        
        Assert.assertEquals(1, count(testTable.getTableName()));
    }
}
