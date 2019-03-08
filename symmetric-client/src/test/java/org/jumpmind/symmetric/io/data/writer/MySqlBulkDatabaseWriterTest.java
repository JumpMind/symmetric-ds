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

import java.util.List;

import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.mysql.MySqlDatabasePlatform;
import org.jumpmind.db.util.BasicDataSourcePropertyConstants;
import org.jumpmind.symmetric.io.MySqlBulkDatabaseWriter;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.StagingManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;

public class MySqlBulkDatabaseWriterTest extends AbstractBulkDatabaseWriterTest {

    protected static IStagingManager stagingManager;

    @BeforeClass
    public static void setup() throws Exception {
        if (DbTestUtils.getEnvironmentSpecificProperties(DbTestUtils.ROOT).get(BasicDataSourcePropertyConstants.DB_POOL_DRIVER)
                .equals("com.mysql.jdbc.Driver")) {
            platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
            platform.createDatabase(platform.readDatabaseFromXml("/testBulkWriter.xml", true), true, false);
            stagingManager = new StagingManager("tmp",false);
        }
    }

    @Before
    public void setupTest() {
        setErrorExpected(false);
    }

    protected boolean shouldTestRun(IDatabasePlatform platform) {
        return platform != null && platform instanceof MySqlDatabasePlatform;
    }

    protected AbstractDatabaseWriter create(){
        return new MySqlBulkDatabaseWriter(platform, platform, "sym_", stagingManager, new CommonsDbcpNativeJdbcExtractor(), 10, 1000,true, true);
    }
    
    protected long writeData(List<CsvData> data) {
        Table table = platform.getTableFromCache(getTestTable(), false);
        return writeData(new MySqlBulkDatabaseWriter(platform, platform, "sym_", stagingManager, new CommonsDbcpNativeJdbcExtractor(), 10, 1000,
                true, true), new TableCsvData(table, data));
    }

}
