package org.jumpmind.symmetric;

import javax.sql.DataSource;

import org.hsqldb.types.Types;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.io.FileUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.core.sql.ISqlTransaction;
import org.jumpmind.symmetric.jdbc.datasource.DriverDataSourceProperties;
import org.jumpmind.symmetric.jdbc.db.IJdbcDbPlatform;
import org.jumpmind.symmetric.jdbc.db.JdbcDbPlatformFactory;
import org.junit.BeforeClass;

abstract public class AbstractDatabaseTest {

    final protected Log log = LogFactory.getLog(getClass());

    static protected IJdbcDbPlatform platform;

    protected static IJdbcDbPlatform getPlatform() {
        return getPlatform(true);
    }

    protected static IJdbcDbPlatform getPlatform(boolean useExisting) {
        IJdbcDbPlatform result = null;
        if (useExisting) {
            if (platform == null) {
                platform = JdbcDbPlatformFactory.createPlatform(createDataSource(),
                        new Parameters());
            }
            result = platform;
        } else {
            result = JdbcDbPlatformFactory.createPlatform(createDataSource(), new Parameters());
        }
        return result;
    }

    @BeforeClass
    public static void setupDataSource() {
        FileUtils.deleteDirectory("target/h2");
        platform = getPlatform(false);
    }

    protected static DataSource createDataSource() {
        return new DriverDataSourceProperties("src/test/resources/test-jdbc.properties")
                .getDataSource();
    }

    protected Table buildTestTable() {
        IJdbcDbPlatform platform = getPlatform(true);
        Table table = new Table("TEST", new Column("TEST_ID", TypeMap.INTEGER, null, true, true,
                true), new Column("TEST_TEXT", TypeMap.VARCHAR, "1000", false, false, false));
        ISqlConnection sqlConnection = platform.getSqlConnection();
        String alterSql = platform.getAlterScriptFor(table);
        sqlConnection.update(alterSql);
        return platform.findTable(table.getTableName());
    }

    protected void delete(String tableName) {
        getPlatform(true).getSqlConnection().update(String.format("delete from %s", tableName));
    }

    protected void insertTestTableRows(int count) {
        ISqlConnection sqlConnection = getPlatform(true).getSqlConnection();
        for (int i = 0; i < count; i++) {
            sqlConnection
                    .update("insert into test (test_text) values('the lazy brown fox jumped over "
                            + i + " logs.')");
        }
    }

    protected int count(String tableName) {
        return count(tableName, null);
    }

    protected int count(String tableName, String where) {
        IDbPlatform platform = getPlatform(false);
        ISqlConnection connection = platform.getSqlConnection();
        return connection.queryForInt(String.format("select count(*) from %s %s %s", tableName,
                StringUtils.isNotBlank(where) ? "where" : "", StringUtils.isNotBlank(where) ? where
                        : ""));
    }

    protected void prepareInsertIntoTestTable(ISqlTransaction transaction, String tableName,
            int flushAt, boolean batchMode) {
        transaction.setInBatchMode(batchMode);
        transaction.setNumberOfRowsBeforeBatchFlush(flushAt);
        transaction.prepare(
                String.format("insert into %s (TEST_ID, TEST_TEXT) values(?, ?)", tableName));
    }

    protected int batchInsertIntoTestTable(int numberToInsert, int numberToStartAt,
            ISqlTransaction transaction) {
        int updatedCount = 0;
        for (int i = 0; i < numberToInsert; i++) {
            updatedCount += transaction.update(numberToStartAt + i, new Object[] {
                    numberToStartAt + i, Integer.toString(numberToStartAt + i) }, new int[] {
                    Types.INTEGER, Types.VARCHAR });
        }
        return updatedCount;
    }

    protected void printOutTable(String tableName) {
        log.log(LogLevel.INFO,
                getPlatform().getSqlConnection()
                        .query(String.format("select * from %s", tableName)).toString());
    }

}
