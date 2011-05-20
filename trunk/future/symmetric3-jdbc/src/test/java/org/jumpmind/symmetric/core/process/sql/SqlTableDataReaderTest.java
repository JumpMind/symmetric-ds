package org.jumpmind.symmetric.core.process.sql;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.junit.Test;

public class SqlTableDataReaderTest extends AbstractDatabaseTest {

    @Test
    public void testSimpleTableWithNoCondition() throws Exception {
        Table testTable = buildTestTable();
        insertTestTableRows(100);
        TableToExtract tableToExtract = new TableToExtract(testTable, "");
        SqlTableDataReader reader = new SqlTableDataReader(getPlatform(true), new Batch(),
                tableToExtract);
        DataContext ctx = reader.createDataContext();
        reader.open(ctx);
        Batch batch = reader.nextBatch(ctx);
        Assert.assertNotNull(batch);
        Table nextTable = reader.nextTable(ctx);
        Assert.assertNotNull(nextTable);
        Assert.assertEquals(testTable, nextTable);
        for (int i = 0; i < 100; i++) {
            Data data = reader.nextData(ctx);
            Assert.assertNotNull("Null data on the " + i + " element", data);
        }
        Data data = reader.nextData(ctx);
        Assert.assertNull(data);
    }
}
