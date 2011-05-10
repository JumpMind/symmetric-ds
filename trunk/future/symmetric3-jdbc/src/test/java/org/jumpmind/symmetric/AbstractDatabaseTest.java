package org.jumpmind.symmetric;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.io.FileUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.jdbc.datasource.DriverDataSourceProperties;
import org.jumpmind.symmetric.jdbc.db.IJdbcDbPlatform;
import org.jumpmind.symmetric.jdbc.db.JdbcDbPlatformFactory;
import org.junit.BeforeClass;

abstract public class AbstractDatabaseTest {

    static protected DataSource dataSource;

    protected IJdbcDbPlatform getPlatform() {
        return JdbcDbPlatformFactory.createPlatform(dataSource, new Parameters());
    }
    
    @BeforeClass
    public static void setupDataSource() {
        FileUtils.deleteDirectory("target/h2");
        dataSource = new DriverDataSourceProperties("src/test/resources/test-jdbc.properties")
                .getDataSource();
    }
    
    protected Table buildTestTable() {
        IJdbcDbPlatform platform = getPlatform();
        Table table = new Table("TEST",
                new Column("TEST_ID", TypeMap.INTEGER, null, true, true, true),
                new Column("TEST_TEXT", TypeMap.VARCHAR, "1000", false, false, false));
        ISqlConnection sqlConnection = platform.getSqlConnection();
        String alterSql = platform.getAlterScriptFor(table);
        sqlConnection.update(alterSql);
        return getPlatform().findTable(table.getTableName());
    }
    
    protected void insertTestTableRows(int count) {
        ISqlConnection sqlConnection = getPlatform().getSqlConnection();
        for (int i = 0; i < count; i++) {
            sqlConnection.update("insert into test (test_text) values('the lazy brown fox jumped over " + i + " logs.')");
        }
    }
}
