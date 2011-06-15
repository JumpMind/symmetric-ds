package org.jumpmind.symmetric.core.process.sql;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.junit.Before;
import org.junit.Test;

public class SqlTableDataReaderTest extends AbstractDatabaseTest {

    Table testTable;

    @Before
    public void cleanupTestTable() {
        testTable = buildTestTable();
        delete(testTable.getTableName());
    }

    @Test
    public void testSimpleTableWithNoCondition() throws Exception {
        insertTestTableRows(100);
        TableToExtract tableToExtract = new TableToExtract(testTable, "");
        SqlTableDataReader reader = new SqlTableDataReader(getDbDialect(true), new Batch(),
                tableToExtract);
        DataContext ctx = new DataContext();
        reader.open(ctx);
        Batch batch = reader.nextBatch();
        Assert.assertNotNull(batch);
        Table nextTable = reader.nextTable();
        Assert.assertNotNull(nextTable);
        Assert.assertEquals(testTable, nextTable);
        for (int i = 0; i < 100; i++) {
            Data data = reader.nextData();
            Assert.assertNotNull("Null data on the " + i + " element", data);
        }
        Data data = reader.nextData();
        Assert.assertNull(data);
    }
}
