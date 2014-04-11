package org.jumpmind.symmetric.io.data.writer;

import java.util.List;

import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSqlDatabasePlatform;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;

public class MsSqlBulkDatabaseWriterTest extends AbstractBulkDatabaseWriterTest {

    protected static IStagingManager stagingManager;

    @BeforeClass
    public static void setup() throws Exception {
        if (DbTestUtils.getEnvironmentSpecificProperties(DbTestUtils.ROOT).get(BasicDataSourcePropertyConstants.DB_POOL_DRIVER)
                .equals("net.sourceforge.jtds.jdbc.Driver")) {
            platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
            platform.createDatabase(platform.readDatabaseFromXml("/testBulkWriter.xml", true), true, false);
            stagingManager = new StagingManager("tmp");
        }
    }

    @Before
    public void setupTest() {
        setErrorExpected(false);
    }

    protected boolean shouldTestRun(IDatabasePlatform platform) {
        return false;
        //return platform != null && platform instanceof MsSqlDatabasePlatform;
    }

    protected long writeData(List<CsvData> data) {
        Table table = platform.getTableFromCache(getTestTable(), false);
        return writeData(new MsSqlBulkDatabaseWriter(platform, stagingManager, new CommonsDbcpNativeJdbcExtractor(), 1000, false), new TableCsvData(table, data));
    }

}