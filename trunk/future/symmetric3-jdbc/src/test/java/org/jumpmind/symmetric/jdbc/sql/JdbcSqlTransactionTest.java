package org.jumpmind.symmetric.jdbc.sql;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.core.sql.ISqlTransaction;
import org.junit.Before;
import org.junit.Test;

public class JdbcSqlTransactionTest extends AbstractDatabaseTest {

    Table testTable;

    @Before
    public void cleanupTestTable() {
        testTable = buildTestTable();
        getPlatform(true).getSqlConnection().update(
                String.format("delete from %s", testTable.getTableName()));
    }

    @Test
    public void testSuccessfulBatchInserts() {
        IDbPlatform platform = getPlatform(true);
        ISqlConnection connection = platform.getSqlConnection();
        ISqlTransaction transaction = connection.startSqlTransaction();
        int flushAt = 10;
        transaction.prepare(
                String.format("insert into %s (TEST_TEXT) values(?)", testTable.getTableName()),
                flushAt, true);
        Assert.assertEquals(0, insert(5, 1, transaction));
        Assert.assertEquals(5, transaction.flush());
        Assert.assertEquals(flushAt, insert(flushAt, 6, transaction));
        //Assert.assertEquals(0, count(testTable.getTableName()));

        transaction.commit();
        transaction.close();

        Assert.assertEquals(15, count(testTable.getTableName()));
    }
    
    @Test
    public void testRollbackBatchInserts() {
        IDbPlatform platform = getPlatform(true);
        ISqlConnection connection = platform.getSqlConnection();
        ISqlTransaction transaction = connection.startSqlTransaction();
        int flushAt = 11;
        transaction.prepare(
                String.format("insert into %s (TEST_TEXT) values(?)", testTable.getTableName()),
                flushAt, true);
        Assert.assertEquals(flushAt, insert(12, 1, transaction));
        Assert.assertEquals(1, transaction.flush());

        transaction.rollback();
        transaction.close();

        Assert.assertEquals(0, count(testTable.getTableName()));
    }    

}
