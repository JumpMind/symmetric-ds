/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.io;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.platform.oracle.OracleDatabasePlatform;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

public class OracleBulkDatabaseWriterTest extends AbstractWriterTest {

	protected static IStagingManager stagingManager;
	
    @BeforeClass
    public static void setup() throws Exception {
        if (DbTestUtils.getEnvironmentSpecificProperties(DbTestUtils.ROOT)
                .get(BasicDataSourcePropertyConstants.DB_POOL_DRIVER)
                .equals("oracle.jdbc.driver.OracleDriver")) {
            platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
            platform.createDatabase(
                    platform.readDatabaseFromXml("/testOracleBulkWriter.xml", true), true, false);
            stagingManager = new StagingManager("tmp",false);
        }
    }

    @Before
    public void setupTest() {
        setErrorExpected(false);
    }

    @Override
    protected long writeData(TableCsvData... datas) {
    	EnvironmentSpecificProperties prop = DbTestUtils.getEnvironmentSpecificProperties(DbTestUtils.ROOT);
    	if(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_SQLLDR_CMD.startsWith("oracle.")) {
    	    // The oracle. gets stripped from the property keyname, need to add it back
            if(prop.get(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_SQLLDR_CMD.replace("oracle.", "")) != null) {
                prop.put(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_SQLLDR_CMD, prop.get(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_SQLLDR_CMD.replace("oracle.", "")));
            }

    	}
        return writeData(new OracleBulkDatabaseWriter(platform, platform, stagingManager, "sym_",
        		prop.get(ParameterConstants.DBDIALECT_ORACLE_BULK_LOAD_SQLLDR_CMD),
        		"silent=(header,discards) direct=false readsize=4096000 bindsize=4096000 rows=1000 discardmax=1 errors=0",
        		prop.get(BasicDataSourcePropertyConstants.DB_POOL_USER),
        		prop.get(BasicDataSourcePropertyConstants.DB_POOL_PASSWORD), 
        		prop.get(BasicDataSourcePropertyConstants.DB_POOL_URL), null, null), datas);
    }

    @Override
    protected String getTestTable() {
        return "test_bulkload_table_1";
    }

    @Test
    public void testInsert1000Rows() {
        if (platform != null && platform instanceof OracleDatabasePlatform) {
            platform.getSqlTemplate().update("truncate table test_bulkload_table_1");

            List<CsvData> datas = new ArrayList<CsvData>();
            for (int i = 0; i < 1000; i++) {
                String[] values = { getNextId(), "string2", "string not null2", "char2",
                        "char not null2", "2007-01-02 03:20:10.000", "2007-02-03 04:05:06.000", "0",
                        "47", "67.89", "-0.0747663" };
                CsvData data = new CsvData(DataEventType.INSERT, values);
                datas.add(data);
            }

            long count = writeData(new TableCsvData(platform.getTableFromCache(
                    "test_bulkload_table_1", false), datas));

            Assert.assertEquals(count, countRows("test_bulkload_table_1"));
        }
    }
    
    @Test
    public void testInsertTimestampTZ_timestamp() throws Exception {
        if (platform != null && platform instanceof OracleDatabasePlatform) {

            NativeJdbcExtractor jdbcExtractor = new CommonsDbcpNativeJdbcExtractor();

            platform.getSqlTemplate().update("truncate table test_bulkload_table_1");

            List<CsvData> datas = new ArrayList<CsvData>();

            String id = getNextId();

            String[] values = { id, "string2", "string not null2", "char2",
                    "char not null2", "2007-01-02 03:20:10.000", "2007-02-03 04:05:06.000", "0",
                    "47", "67.89", "-0.0747663", "2007-01-02 03:20:10.000", "2007-01-02 03:20:10.000", 
                    "2007-01-02 03:20:10.000", "2007-01-02 03:20:10.000" };
            CsvData data = new CsvData(DataEventType.INSERT, values);
            datas.add(data);

            long count = writeData(new TableCsvData(platform.getTableFromCache(
                    "test_bulkload_table_1", false), datas));

            Map<String, Object> rowData = queryForRow(id);
            DataSource datasource = (DataSource)platform.getDataSource();
            Connection connection = datasource.getConnection();
            Connection oracleConnection = jdbcExtractor.getNativeConnection(connection);

            final String[] EXPECTED_TIMESTAMPTZ = {"2007-01-02 03:20:10.0 -5:00","2007-01-02 03:20:10.0 -4:00"};

            checkTimestampTZ(rowData.get("TIMESTAMPTZ0_VALUE"), oracleConnection, EXPECTED_TIMESTAMPTZ);
            checkTimestampTZ(rowData.get("TIMESTAMPTZ3_VALUE"), oracleConnection, EXPECTED_TIMESTAMPTZ);
            checkTimestampTZ(rowData.get("TIMESTAMPTZ6_VALUE"), oracleConnection, EXPECTED_TIMESTAMPTZ);
            checkTimestampTZ(rowData.get("TIMESTAMPTZ9_VALUE"), oracleConnection, EXPECTED_TIMESTAMPTZ);

            Assert.assertEquals(count, countRows("test_bulkload_table_1"));
        }
    }
    
    @Test
    public void testInsertTimestampTZ_timestampWithTimeZone() throws Exception {
        if (platform != null && platform instanceof OracleDatabasePlatform) {

            NativeJdbcExtractor jdbcExtractor = new CommonsDbcpNativeJdbcExtractor();

            platform.getSqlTemplate().update("truncate table test_bulkload_table_1");

            List<CsvData> datas = new ArrayList<CsvData>();

            String id = getNextId();

            String[] values = { id, "string2", "string not null2", "char2",
                    "char not null2", "2007-01-02 03:20:10.000", "2007-02-03 04:05:06.000", "0",
                    "47", "67.89", "-0.0747663", "2007-01-02 03:20:10.123456789 -08:00", "2007-01-02 03:20:10.123456789 -08:00", 
                    "2007-01-02 03:20:10.123456789 -08:00", "2007-01-02 03:20:10.123456789 -08:00" };
            CsvData data = new CsvData(DataEventType.INSERT, values);
            datas.add(data);

            long count = writeData(new TableCsvData(platform.getTableFromCache(
                    "test_bulkload_table_1", false), datas));

            Map<String, Object> rowData = queryForRow(id);
            DataSource datasource = (DataSource)platform.getDataSource();
            Connection connection = datasource.getConnection();
            Connection oracleConnection = jdbcExtractor.getNativeConnection(connection);

            checkTimestampTZ(rowData.get("TIMESTAMPTZ0_VALUE"), oracleConnection, "2007-01-02 03:20:10.0 -8:00");
            checkTimestampTZ(rowData.get("TIMESTAMPTZ3_VALUE"), oracleConnection, "2007-01-02 03:20:10.123 -8:00");
            checkTimestampTZ(rowData.get("TIMESTAMPTZ6_VALUE"), oracleConnection, "2007-01-02 03:20:10.123457 -8:00");
            checkTimestampTZ(rowData.get("TIMESTAMPTZ9_VALUE"), oracleConnection, "2007-01-02 03:20:10.123456789 -8:00");

            Assert.assertEquals(count, countRows("test_bulkload_table_1"));
        }
    }
    
    @Test
    public void testInsertTimestampTZ_timestampWithLocalTimeZone() throws Exception {
        if (platform != null && platform instanceof OracleDatabasePlatform) {
            
            NativeJdbcExtractor jdbcExtractor = new CommonsDbcpNativeJdbcExtractor();
            
            platform.getSqlTemplate().update("truncate table test_bulkload_table_1");
            
            List<CsvData> datas = new ArrayList<CsvData>();
            
            String id = getNextId();
            
            String[] values = { id, "string2", "string not null2", "char2",
                    "char not null2", "2007-01-02 03:20:10.000", "2007-02-03 04:05:06.000", "0",
                    "47", "67.89", "-0.0747663", null, null, 
                    null, null, "2007-01-02 03:20:10.123456789 -08:00" };
            CsvData data = new CsvData(DataEventType.INSERT, values);
            datas.add(data);
            
            long count = writeData(new TableCsvData(platform.getTableFromCache(
                    "test_bulkload_table_1", false), datas));
            
            Map<String, Object> rowData = queryForRow(id);
            DataSource datasource = (DataSource)platform.getDataSource();
            Connection connection = datasource.getConnection();
            Connection oracleConnection = jdbcExtractor.getNativeConnection(connection);
            
            checkTimestampLTZ(rowData.get("TIMESTAMPLTZ9_VALUE"), oracleConnection, new String[]{"2007-01-02 06:20:10.123456789 America/New_York","2007-01-02 06:20:10.123456789 US/Eastern"});
            
            Assert.assertEquals(count, countRows("test_bulkload_table_1"));
        }
    }

    /**
     * @param rowData
     * @param oracleConnection
     * @param EXPECTED_TIMESTAMPTZ
     * @throws SQLException
     */
    private void checkTimestampTZ(Object value, Connection oracleConnection, final String... expectedValues)
            throws SQLException {
        TIMESTAMPTZ timestamp = (TIMESTAMPTZ) value;
        String actualTimestampString = timestamp.stringValue(oracleConnection);
        boolean match = false;
        for (String expectedValue : expectedValues) {
            if (StringUtils.equals(expectedValue, actualTimestampString)) {
                match = true;
                break;
            }
        }
        
        Assert.assertTrue(actualTimestampString + 
                " not found in " + Arrays.toString(expectedValues), match);
    }
    
    private void checkTimestampLTZ(Object value, Connection oracleConnection, final String... expectedValues)
            throws SQLException {
        TIMESTAMPLTZ timestamp = (TIMESTAMPLTZ) value;
        String actualTimestampString = timestamp.stringValue(oracleConnection);
        boolean match = false;
        for (String expectedValue : expectedValues) {
            if (StringUtils.equals(expectedValue, actualTimestampString)) {
                match = true;
                break;
            }
        }
        
        Assert.assertTrue(actualTimestampString + 
                " not found in " + Arrays.toString(expectedValues), match);
    }

    @Test
    public void testInsertCollision() {
        if (platform != null && platform instanceof OracleDatabasePlatform) {
            platform.getSqlTemplate().update("truncate table test_bulkload_table_1");

            String id = getNextId();

            String[] values = { id, "string2", "string not null2", "char2",
                    "char not null2", "2007-01-02 03:20:10.000", "2007-02-03 04:05:06.000", "0", "47",
                    "67.89", "-0.0747663" };
            CsvData data = new CsvData(DataEventType.INSERT, values);
            writeData(data, values);
            Assert.assertEquals(1, countRows("test_bulkload_table_1"));


            try {
                setErrorExpected(true);

                List<CsvData> datas = new ArrayList<CsvData>();
                datas.add(data);

                for (int i = 0; i < 10; i++) {
                    values = new String[] { id, "string2", "string not null2", "char2",
                            "char not null2", "2007-01-02 03:20:10.000", "2007-02-03 04:05:06.000",
                            "0", "47", "67.89", "-0.0747663" };
                    data = new CsvData(DataEventType.INSERT, values);
                    datas.add(data);
                }

                // we should collide and rollback
                writeData(new TableCsvData(platform.getTableFromCache("test_bulkload_table_1",
                        false), datas));

                Assert.assertEquals(1, countRows("test_bulkload_table_1"));

            } finally {
                setErrorExpected(false);
            }
        }
    }
}
