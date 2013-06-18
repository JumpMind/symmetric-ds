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

import junit.framework.Assert;

import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.platform.oracle.OracleDatabasePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlDatabasePlatform;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;

public class PostgresBulkDatabaseWriterTest extends AbstractWriterTest {

    @BeforeClass
    public static void setup() throws Exception {
        platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
        platform.createDatabase(platform.readDatabaseFromXml("/testBulkWriter.xml", true), true,
                false);
    }

    @Before
    public void setupTest() {
        setErrorExpected(false);
    }

    @Override
    protected long writeData(TableCsvData... datas) {
        return writeData(new PostgresBulkDatabaseWriter(platform,
                new CommonsDbcpNativeJdbcExtractor(), 1000), datas);
    }

    @Override
    protected String getTestTable() {
        return "test_bulkload_table_1";
    }

    @Test
    public void testInsert1000Rows() {
        if (platform instanceof PostgreSqlDatabasePlatform) {
            platform.getSqlTemplate().update("truncate table test_bulkload_table_1");

            List<CsvData> datas = new ArrayList<CsvData>();
            for (int i = 0; i < 1000; i++) {
                String[] values = { getNextId(), "stri'ng2", "string not null2", "char2",
                        "char not null2", "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0",
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
    public void testInsertCollision() {
        if (platform instanceof OracleDatabasePlatform) {
            platform.getSqlTemplate().update("truncate table test_bulkload_table_1");

            String[] values = { getNextId(), "string2", "string not null2", "char2",
                    "char not null2", "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0", "47",
                    "67.89", "-0.0747663" };
            CsvData data = new CsvData(DataEventType.INSERT, values);
            writeData(data, values);
            Assert.assertEquals(1, countRows("test_bulkload_table_1"));

            try {
                setErrorExpected(true);

                List<CsvData> datas = new ArrayList<CsvData>();
                datas.add(data);
                for (int i = 0; i < 10; i++) {
                    values = new String[] { getNextId(), "string2", "string not null2", "char2",
                            "char not null2", "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0",
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
