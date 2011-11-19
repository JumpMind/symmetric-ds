/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.load;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.random.RandomDataImpl;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.informix.InformixDbDialect;
import org.jumpmind.symmetric.db.mssql.MsSqlDbDialect;
import org.jumpmind.symmetric.db.mysql.MySqlDbDialect;
import org.jumpmind.symmetric.db.oracle.OracleDbDialect;
import org.jumpmind.symmetric.db.postgresql.PostgreSqlDbDialect;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.junit.Assert;
import org.junit.Test;

public class DataLoaderTest extends AbstractDataLoaderTest {

    public DataLoaderTest() throws Exception {
    }
    
    @Test
    public void testInsertExisting() throws Exception {
        String[] values = { getNextId(), "string2", "string not null2", "char2", "char not null2",
                "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0", "47", "67.89",
                "-0.0747663" };
        massageExpectectedResultsForDialect(values);
        testSimple(CsvConstants.INSERT, values, values);

        values[1] = "insert fallback to update";
        massageExpectectedResultsForDialect(values);
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testLargeDouble() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[10] = "-0.07476635514018691588785046728971962617";
        String[] expectedValues = (String[]) ArrayUtils.clone(values);
        massageExpectectedResultsForDialect(expectedValues);
        testSimple(CsvConstants.INSERT, values, expectedValues);
    }

    @Test
    public void testDecimalLocale() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[10] = "123456,99";
        String[] expectedValues = (String[]) ArrayUtils.clone(values);
        massageExpectectedResultsForDialect(expectedValues);
        testSimple(CsvConstants.INSERT, values, expectedValues);
    }

    @Test
    public void testUpdateNotExisting() throws Exception {
        String id = getNextId();
        String[] values = { id, "it's /a/  string", "it's  -not-  null", "You're a \"character\"", "Where are you?",
                "2007-12-31 02:33:45.0", "2007-12-31 23:59:59.0", "1", "13", "9.95", "-0.0747", id };
        String[] expectedValues = (String[]) ArrayUtils.subarray(values, 0, values.length - 1);
        massageExpectectedResultsForDialect(expectedValues);
        testSimple(CsvConstants.UPDATE, values, expectedValues);
    }

    @Test
    public void testStringQuotes() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[1] = "It's \"quoted,\" with a comma";
        values[2] = "two 'ticks'";
        values[3] = "One quote\"";
        values[4] = "One comma,";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringSpaces() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[1] = "  two spaces before";
        values[2] = "two spaces after  ";
        values[3] = " one space before";
        values[4] = "one space after ";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringOneSpace() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[2] = values[4] = " ";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringEmpty() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[1] = values[2] = values[3] = values[4] = "";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringNull() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringBackslash() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[1] = "Here's a \\, a (backslash)";
        values[2] = "Fix TODO";
        // TODO: Fix backslashing alphanumeric
        // values[2] = "\\a\\b\\c\\ \\1\\2\\3";
        values[3] = "Tick quote \\'\\\"";
        values[4] = "Comma quote \\,\\\"";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testDeleteExisting() throws Exception {
        String[] values = { getNextId(), "a row to be deleted", "testDeleteExisting", "char2", "char not null2",
                "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0", "47", "67.89", "-0.0747" };
        massageExpectectedResultsForDialect(values);
        testSimple(CsvConstants.INSERT, values, values);
        testSimple(CsvConstants.DELETE, new String[] { getId() }, null);
    }

    @Test
    public void testDeleteNotExisting() throws Exception {
        testSimple(CsvConstants.DELETE, new String[] { getNextId() }, null);
    }

    @Test
    public void testColumnNotExisting() throws Exception {
        String[] columns = (String[]) ArrayUtils.add(TEST_COLUMNS, "Unknown_Column");
        String[] values = { getNextId(), "testColumnNotExisting", "string not null", "char", "char not null",
                "2007-01-02 00:00:00.0", "2007-02-03 04:05:06.0", "0", "47", "67.89", "-0.0747", "i do not exist!" };
        String[] expectedValues = (String[]) ArrayUtils.subarray(values, 0, values.length - 1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writeTable(writer, TEST_TABLE, TEST_KEYS, columns);
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);
        massageExpectectedResultsForDialect(expectedValues);
        assertTestTableEquals(values[0], expectedValues);
    }

    @Test
    public void testTableNotExisting() throws Exception {
        String tableName = "UnknownTable";
        String[] keys = { "id" };
        String[] columns = { "id", "name" };
        String[] badValues = { "1", "testTableNotExisting" };
        String[] values = { getNextId(), "testTableNotExisting", "This row should load", "char", "char not null",
                "2007-01-02 00:00:00.0", "2007-02-03 04:05:06.0", "0", "0", "12.10", "-0.0747" };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writeTable(writer, tableName, keys, columns);
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(badValues, true);
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);
        massageExpectectedResultsForDialect(values);
        assertTestTableEquals(values[0], values);
    }

    @Test
    public void testLargeColumn() throws Exception {
        String tableName = "UnknownTable";
        String[] keys = { "id" };
        String[] columns = { "id", "name" };
        String[] values = { "1", new RandomDataImpl().nextSecureHexString(110000) };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writeTable(writer, tableName, keys, columns);
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.close();
        load(out);
    }

    private void massageExpectectedResultsForDialect(String[] values) {
        if (values[5] != null && (!(getDbDialect() instanceof OracleDbDialect || getDbDialect() instanceof MsSqlDbDialect))) {
            values[5] = values[5].replaceFirst(" \\d\\d:\\d\\d:\\d\\d\\.?0?", " 00:00:00.0");
        }
        if (values[10] != null) {
            values[10] = values[10].replace(',', '.');
        }
        if (values[10] != null && !(getDbDialect() instanceof OracleDbDialect)) {
            int scale = 17;
            if (getDbDialect() instanceof MySqlDbDialect) {
                scale = 16;
            }
            DecimalFormat df = new DecimalFormat("0.00####################################");
            values[10] = df.format(new BigDecimal(values[10]).setScale(scale, RoundingMode.DOWN));
        }
    }

    @Test
    public void testColumnLevelSync() throws Exception {
        String[] insertValues = new String[TEST_COLUMNS.length];
        insertValues[2] = insertValues[4] = "column sync";
        insertValues[0] = getNextId();
        String[] updateValues = new String[3];
        updateValues[0] = updateValues[2] = insertValues[0];
        updateValues[1] = "new value";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);

        // Clean insert
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        // update a single column
        String[] columns = { "id", "string_value" };
        writeTable(writer, TEST_TABLE, TEST_KEYS, columns);
        writer.write(CsvConstants.UPDATE);
        writer.writeRecord(updateValues, true);

        // update a single column
        columns = new String[] { "id", "char_value" };
        writeTable(writer, TEST_TABLE, TEST_KEYS, columns);
        writer.write(CsvConstants.UPDATE);
        writer.writeRecord(updateValues, true);

        writer.close();
        load(out);

        insertValues[1] = updateValues[1];
        insertValues[3] = updateValues[1];
        assertTestTableEquals(insertValues[0], insertValues);
    }

    @Test
    public void testBinaryColumnTypesForPostgres() throws Exception {
        if (getDbDialect() instanceof PostgreSqlDbDialect) {
            getJdbcTemplate().update("drop table if exists test_postgres_binary_types");
            getJdbcTemplate().update("create table test_postgres_binary_types (binary_data oid)");

            String tableName = "test_postgres_binary_types";
            String[] keys = {"binary_data"};
            String[] columns = {"binary_data"};
            String[] values = {"dGVzdCAxIDIgMw=="};

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            CsvWriter writer = getWriter(out);
            writer.writeRecord(new String[] {CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID});
            writeTable(writer, tableName, keys, columns);
            String nextBatchId = getNextBatchId();
            writer.writeRecord(new String[] {CsvConstants.BATCH, nextBatchId});
            writer.writeRecord(new String[] {CsvConstants.BINARY, BinaryEncoding.BASE64.name()});
            writer.write(CsvConstants.INSERT);
            writer.writeRecord(values, true);
            writer.writeRecord(new String[] {CsvConstants.COMMIT, nextBatchId});
            writer.close();
            load(out);

            String result = (String) getJdbcTemplate().queryForObject(
                "select data from pg_largeobject where loid in (select binary_data from test_postgres_binary_types)",
                String.class);
            // clean up the object from pg_largeobject, otherwise it becomes abandoned on subsequent runs
            getJdbcTemplate().queryForList("select lo_unlink(binary_data) from test_postgres_binary_types");
            assertEquals(result, "test 1 2 3");
        }
    }

    @Test
    public void testBenchmark() throws Exception {
        ZipInputStream in = new ZipInputStream(getClass().getResourceAsStream("/test-data-loader-benchmark.zip"));
        in.getNextEntry();
        long startTime = System.currentTimeMillis();
        IDataLoader dataLoader = getDataLoader();       
        try {
            dataLoader.open(TransportUtils.toReader(in), getDataSource(), null);
            while (dataLoader.hasNext()) {
                dataLoader.load();
            }
        } finally {
            dataLoader.close();
        }
        double totalSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
        
        double targetTime = 15.0;
        if (getDbDialect() instanceof InformixDbDialect) {
            targetTime = 20.0;
        }
        
        // TODO: this used to run in 1 second; can we do some optimization?
        Assert.assertTrue("DataLoader running in " + totalSeconds + " is too slow", totalSeconds <= targetTime);
    }    

}