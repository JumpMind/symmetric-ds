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
package org.jumpmind.symmetric.io.data.writer;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2000DatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2005DatabasePlatform;
import org.jumpmind.db.platform.mssql.MsSql2008DatabasePlatform;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.symmetric.io.MsSqlBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;

@SuppressWarnings("deprecation")
public class MsSqlBulkDatabaseWriterTest extends AbstractBulkDatabaseWriterTest {

    protected static IStagingManager stagingManager;
    
    protected static final String uncPath = null;

    @BeforeClass
    public static void setup() throws Exception {
        if (DbTestUtils.getEnvironmentSpecificProperties(DbTestUtils.ROOT).get(BasicDataSourcePropertyConstants.DB_POOL_DRIVER)
                .equals("net.sourceforge.jtds.jdbc.Driver")) {
            platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
            platform.createDatabase(platform.readDatabaseFromXml("/testBulkWriter.xml", true), true, false);
            stagingManager = new StagingManager("target/tmp",false);
        }
    }

    @Before
    public void setupTest() {
        setErrorExpected(false);
    }

    protected boolean shouldTestRun(IDatabasePlatform platform) {
        return platform != null && (
                platform instanceof MsSql2000DatabasePlatform || 
                platform instanceof MsSql2005DatabasePlatform || 
                platform instanceof MsSql2008DatabasePlatform);
    }

    protected AbstractDatabaseWriter create(){
        return new MsSqlBulkDatabaseWriter(platform, platform, "sym_", stagingManager, new CommonsDbcpNativeJdbcExtractor(), 1000, false, uncPath, null, null, null);
    }
    
    protected long writeData(List<CsvData> data) {
        Table table = platform.getTableFromCache(getTestTable(), false);
        return writeData(new MsSqlBulkDatabaseWriter(platform, platform, "sym_", stagingManager, new CommonsDbcpNativeJdbcExtractor(), 1000, false, uncPath, null, null, null), new TableCsvData(table, data));
    }

    @Test
    public void testInsertReorderColumns() throws Exception {
        if (shouldTestRun(platform)) {
            String id = getNextId();
            String[] values = { "string with space in it", "string-with-no-space", "string with space in it",
                    "string-with-no-space", "2007-01-02 00:00:00.000", "2007-02-03 04:05:06.000", "0", "47", "67.89", "-0.0747663",
                    encode("string with space in it"), id };
            List<CsvData> data = new ArrayList<CsvData>();
            data.add(new CsvData(DataEventType.INSERT, (String[]) ArrayUtils.clone(values)));
            Table table = (Table) platform.getTableFromCache(getTestTable(), false).clone();
            Column firstColumn = table.getColumn(0);
            table.removeColumn(firstColumn);
            table.addColumn(firstColumn);
            writeData(new MsSqlBulkDatabaseWriter(platform, platform, "sym_", stagingManager, new CommonsDbcpNativeJdbcExtractor(), 1000, false, uncPath, null, null, null), 
                    new TableCsvData(table, data));
            values = (String[]) ArrayUtils.remove(values, values.length - 1);
            values = (String[]) ArrayUtils.add(values, 0, id);
            assertTestTableEquals(id, values);
        }
    }

}