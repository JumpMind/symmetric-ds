package org.jumpmind.symmetric.jdbc.sql;

import junit.framework.Assert;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.core.db.DataIntegrityViolationException;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.ISqlTransaction;
import org.jumpmind.symmetric.core.model.Table;
import org.junit.Before;
import org.junit.Test;

public class JdbcSqlTransactionTest extends AbstractDatabaseTest {

    Table testTable;

    @Before
    public void cleanupTestTable() {
        testTable = buildTestTable();
        delete(testTable.getTableName());
    }

    @Test
    public void testSuccessfulBatchInserts() {
        IDbDialect platform = getDbDialect(true);
        ISqlTemplate connection = platform.getSqlTemplate();
        ISqlTransaction transaction = connection.startSqlTransaction();
        int flushAt = 10;
        prepareInsertIntoTestTable(transaction, testTable.getTableName(), flushAt, true);
        Assert.assertEquals(0, batchInsertIntoTestTable(5, 1, transaction));
        Assert.assertEquals(5, transaction.flush());
        Assert.assertEquals(flushAt, batchInsertIntoTestTable(flushAt, 6, transaction));
        // it would be nice to be able to test that the rows are not yet
        // committed, but in single connection databases the database is still
        // locked at this point.
        // Assert.assertEquals(0, count(testTable.getTableName()));
        Assert.assertEquals(0, transaction.getUnflushedMarkers(true).size());
        transaction.commit();
        transaction.close();

        Assert.assertEquals(15, count(testTable.getTableName()));
    }

    @Test
    public void testRollbackBatchInserts() {
        IDbDialect platform = getDbDialect(true);
        ISqlTemplate connection = platform.getSqlTemplate();
        ISqlTransaction transaction = connection.startSqlTransaction();
        int flushAt = 11;
        prepareInsertIntoTestTable(transaction, testTable.getTableName(), flushAt, true);
        Assert.assertEquals(flushAt, batchInsertIntoTestTable(12, 1, transaction));
        Assert.assertEquals(1, transaction.getUnflushedMarkers(false).size());
        Assert.assertEquals(1, transaction.flush());
        Assert.assertEquals(0, transaction.getUnflushedMarkers(false).size());
        transaction.rollback();
        transaction.close();

        Assert.assertEquals(0, count(testTable.getTableName()));
    }

    @Test
    public void testDataIntegrityViolationInBatchMode() {
        IDbDialect platform = getDbDialect(true);
        ISqlTemplate connection = platform.getSqlTemplate();
        ISqlTransaction transaction = connection.startSqlTransaction();
        int flushAt = 10;
        prepareInsertIntoTestTable(transaction, testTable.getTableName(), flushAt, true);
        Assert.assertEquals(flushAt, batchInsertIntoTestTable(10, 1, transaction));
        Assert.assertEquals(0, transaction.getUnflushedMarkers(false).size());
        prepareInsertIntoTestTable(transaction, testTable.getTableName(), flushAt, true);
        
        int unflushedMarkers = 0;
        try {
            Assert.assertEquals(0, batchInsertIntoTestTable(2, 11, transaction));
            Assert.assertEquals(0, batchInsertIntoTestTable(5, 9, transaction));
            transaction.flush();
            Assert.fail("This should have failed");
        } catch (DataIntegrityViolationException ex) {
            unflushedMarkers = transaction.getUnflushedMarkers(false).size();
            Assert.assertTrue(
                    "We expected there to be at least 4 unflushed elements.  Instead there were "
                            + unflushedMarkers, unflushedMarkers >= 4);
        }

        transaction.commit();
        Assert.assertEquals(17-unflushedMarkers, count(testTable.getTableName()));
    }

    @Test
    public void testNonBatchSuccessfulUpdates() {
        IDbDialect platform = getDbDialect(true);
        ISqlTemplate connection = platform.getSqlTemplate();
        ISqlTransaction transaction = connection.startSqlTransaction();
        prepareInsertIntoTestTable(transaction, testTable.getTableName(), -1, false);
        Assert.assertEquals(5, batchInsertIntoTestTable(5, 1, transaction));
        Assert.assertEquals(0, transaction.flush());
        Assert.assertEquals(5, batchInsertIntoTestTable(5, 6, transaction));
        // it would be nice to be able to test that the rows are not yet
        // committed, but in single connection databases the database is still
        // locked at this point.
        // Assert.assertEquals(0, count(testTable.getTableName()));
        Assert.assertEquals(0, transaction.getUnflushedMarkers(false).size());
        transaction.commit();
        transaction.close();

        Assert.assertEquals(10, count(testTable.getTableName()));
    }

    @Test
    public void testDataIntegrityViolationInNonBatchMode() {
        IDbDialect platform = getDbDialect(true);
        ISqlTemplate connection = platform.getSqlTemplate();
        ISqlTransaction transaction = connection.startSqlTransaction();
        prepareInsertIntoTestTable(transaction, testTable.getTableName(), -1, true);
        Assert.assertEquals(10, batchInsertIntoTestTable(10, 1, transaction));
        Assert.assertEquals(0, transaction.getUnflushedMarkers(false).size());
        prepareInsertIntoTestTable(transaction, testTable.getTableName(), -1, true);
        try {
            Assert.assertEquals(2, batchInsertIntoTestTable(2, 11, transaction));
            Assert.assertEquals(0, batchInsertIntoTestTable(5, 9, transaction));
            Assert.fail("This should have failed");
        } catch (DataIntegrityViolationException ex) {

        }

        Assert.assertEquals(5, batchInsertIntoTestTable(5, 13, transaction));
        transaction.commit();
        Assert.assertEquals(17, count(testTable.getTableName()));
    }

}
