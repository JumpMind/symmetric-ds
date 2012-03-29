package org.jumpmind.symmetric;

import javax.sql.DataSource;

import org.hsqldb.types.Types;
import org.jumpmind.symmetric.core.common.FileUtils;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.ISqlTransaction;
import org.jumpmind.symmetric.core.db.SqlScript;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.jdbc.datasource.DriverDataSourceProperties;
import org.jumpmind.symmetric.jdbc.db.IJdbcDbDialect;
import org.jumpmind.symmetric.jdbc.db.JdbcDbDialectFactory;
import org.junit.BeforeClass;

abstract public class AbstractDatabaseTest {

    final protected Log log = LogFactory.getLog(getClass());

    static protected IJdbcDbDialect platform;

    public static IJdbcDbDialect getDbDialect() {
        return getDbDialect(true);
    }

    protected static IJdbcDbDialect getDbDialect(boolean useExisting) {
        IJdbcDbDialect result = null;
        if (useExisting) {
            if (platform == null) {
                platform = JdbcDbDialectFactory
                        .createPlatform(createDataSource(), new Parameters());
            }
            result = platform;
        } else {
            result = JdbcDbDialectFactory.createPlatform(createDataSource(), new Parameters());
        }
        return result;
    }

    @BeforeClass
    public static void setupDataSource() {
        FileUtils.deleteDirectory("target/h2");
        platform = getDbDialect(false);
    }

    protected static DataSource createDataSource() {
        return new DriverDataSourceProperties("src/test/resources/test-jdbc.properties")
                .getDataSource();
    }

    protected Table buildTestTable() {
        IJdbcDbDialect platform = getDbDialect(true);
        Table table = new Table("TEST", new Column("TEST_ID", TypeMap.INTEGER, null, true, true,
                true), new Column("TEST_TEXT", TypeMap.VARCHAR, "1000", false, false, false));
        String alterSql = platform.getAlterScriptFor(table);
        SqlScript script = new SqlScript(alterSql, platform);
        script.execute();
        return platform.findTable(table.getTableName(), false);
    }

    protected void delete(String tableName) {
        IDbDialect dbDialect = getDbDialect(true);
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        dbDialect.getSqlTemplate().update(
                String.format("delete from %s%s%s", quoteString, tableName, quoteString));
    }

    protected void insertTestTableRows(int count) {
        ISqlTemplate sqlConnection = getDbDialect(true).getSqlTemplate();
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
        IDbDialect dbDialect = getDbDialect(false);
        ISqlTemplate connection = dbDialect.getSqlTemplate();
        String quoteString = dbDialect.getDbDialectInfo().getIdentifierQuoteString();
        return connection.queryForInt(String.format("select count(*) from %s%s%s %s %s",
                quoteString, tableName, quoteString, StringUtils.isNotBlank(where) ? "where" : "",
                StringUtils.isNotBlank(where) ? where : ""));
    }

    protected void prepareInsertIntoTestTable(ISqlTransaction transaction, String tableName,
            int flushAt, boolean batchMode) {
        transaction.setInBatchMode(batchMode);
        transaction.setNumberOfRowsBeforeBatchFlush(flushAt);
        transaction.prepare(String.format("insert into %s (TEST_ID, TEST_TEXT) values(?, ?)",
                tableName));
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
                getDbDialect().getSqlTemplate().query(String.format("select * from %s", tableName))
                        .toString());
    }

}
