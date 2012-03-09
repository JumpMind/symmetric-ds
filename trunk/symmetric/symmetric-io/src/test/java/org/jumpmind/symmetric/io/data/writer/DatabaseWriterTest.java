package org.jumpmind.symmetric.io.data.writer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.informix.InformixPlatform;
import org.jumpmind.db.platform.mssql.MsSqlPlatform;
import org.jumpmind.db.platform.mysql.MySqlPlatform;
import org.jumpmind.db.platform.oracle.OraclePlatform;
import org.jumpmind.db.platform.postgresql.PostgreSqlPlatform;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabaseWriterTest extends AbstractWriterTest {

    @BeforeClass
    public static void setup() throws Exception {
        platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
        platform.createDatabase(platform.readDatabaseFromXml("/testDatabaseWriter.xml", true),
                true, false);
    }
    
    @Before
    public void notExpectingError() {
        setErrorExpected(false);
    }

    @Test
    public void testInsertExisting() throws Exception {
        String[] values = { getNextId(), "string2", "string not null2", "char2", "char not null2",
                "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0", "47", "67.89", "-0.0747663" };
        massageExpectectedResultsForDialect(values);
        CsvData data = new CsvData(DataEventType.INSERT, values);
        writeData(data, values);

        values[1] = "insert fallback to update";
        massageExpectectedResultsForDialect(values);
        writeData(data, values);
    }

    @Test
    public void testLargeDouble() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[10] = "-0.07476635514018691588785046728971962617";
        String[] expectedValues = (String[]) ArrayUtils.clone(values);
        massageExpectectedResultsForDialect(expectedValues);
        writeData(new CsvData(DataEventType.INSERT, values), expectedValues);
    }

    @Test
    public void testDecimalLocale() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[10] = "123456,99";
        String[] expectedValues = (String[]) ArrayUtils.clone(values);
        massageExpectectedResultsForDialect(expectedValues);
        writeData(new CsvData(DataEventType.INSERT, values), expectedValues);
    }

    @Test
    public void testUpdateNotExisting() throws Exception {
        String id = getNextId();
        String[] values = { id, "it's /a/  string", "it's  -not-  null", "You're a \"character\"",
                "Where are you?", "2007-12-31 02:33:45.0", "2007-12-31 23:59:59.0", "1", "13",
                "9.95", "-0.0747" };
        String[] expectedValues = (String[]) ArrayUtils.subarray(values, 0, values.length);
        massageExpectectedResultsForDialect(expectedValues);
        writeData(new CsvData(DataEventType.UPDATE, new String[] { id }, values), expectedValues);
    }

    @Test
    public void testStringQuotes() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[1] = "It's \"quoted,\" with a comma";
        values[2] = "two 'ticks'";
        values[3] = "One quote\"";
        values[4] = "One comma,";
        writeData(new CsvData(DataEventType.INSERT, values), values);
    }

    @Test
    public void testStringSpaces() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[1] = "  two spaces before";
        values[2] = "two spaces after  ";
        values[3] = " one space before";
        values[4] = "one space after ";
        writeData(new CsvData(DataEventType.INSERT, values), values);
    }

    @Test
    public void testStringOneSpace() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[2] = values[4] = " ";
        writeData(new CsvData(DataEventType.INSERT, values), values);
    }

    @Test
    public void testStringEmpty() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        values[1] = values[2] = values[3] = values[4] = "";
        writeData(new CsvData(DataEventType.INSERT, values), values);
    }

    @Test
    public void testStringNull() throws Exception {
        String[] values = new String[TEST_COLUMNS.length];
        values[0] = getNextId();
        writeData(new CsvData(DataEventType.INSERT, values), values);
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
        writeData(new CsvData(DataEventType.INSERT, values), values);
    }

    @Test
    public void testDeleteExisting() throws Exception {
        String[] values = { getNextId(), "a row to be deleted", "testDeleteExisting", "char2",
                "char not null2", "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0", "47",
                "67.89", "-0.0747" };
        massageExpectectedResultsForDialect(values);
        writeData(new CsvData(DataEventType.INSERT, values), values);
        writeData(new CsvData(DataEventType.DELETE, new String[] { getId() }, null), null);
    }

    @Test
    public void testDeleteNotExisting() throws Exception {
        writeData(new CsvData(DataEventType.DELETE, new String[] { getNextId() }, null), null);
    }

    @Test
    public void testColumnNotExisting() throws Exception {
        List<String> testColumns = new ArrayList<String>(Arrays.asList(TEST_COLUMNS));
        testColumns.add(4, "Unknown_Column");
        String[] columns = testColumns.toArray(new String[testColumns.size()]);

        String[] values = { getNextId(), "testColumnNotExisting", "string not null", "char",
                "i do not exist!", "char not null", "2007-01-02 00:00:00.0",
                "2007-02-03 04:05:06.0", "0", "47", "67.89", "-0.0747" };
        List<String> valuesAsList = new ArrayList<String>(Arrays.asList(values));
        valuesAsList.remove(4);
        String[] expectedValues = valuesAsList.toArray(new String[valuesAsList.size()]);
        writeData(new CsvData(DataEventType.INSERT, values), expectedValues, columns);
    }

    @Test
    public void testTableNotExisting() throws Exception {
        String[] values = { getNextId(), "testTableNotExisting", "This row should load", "char",
                "char not null", "2007-01-02 00:00:00.0", "2007-02-03 04:05:06.0", "0", "0",
                "12.10", "-0.0747" };

        Table badTable = buildSourceTable("UnknownTable", TEST_KEYS, TEST_COLUMNS);
        writeData(new TableCsvData(badTable, new CsvData(DataEventType.INSERT, values)),
                new TableCsvData(buildSourceTable(TEST_TABLE, TEST_KEYS, TEST_COLUMNS),
                        new CsvData(DataEventType.INSERT, values)));

        massageExpectectedResultsForDialect(values);
        assertTestTableEquals(values[0], values);
    }

    @Test
    public void testColumnLevelSync() throws Exception {
        String[] insertValues = new String[TEST_COLUMNS.length];
        insertValues[2] = insertValues[4] = "column sync";
        insertValues[0] = getNextId();
        String[] updateValues = new String[2];
        updateValues[0] = insertValues[0];
        updateValues[1] = "new value";

        writeData(new CsvData(DataEventType.INSERT, insertValues), insertValues);

        // update a single column
        String[] columns = { "id", "string_value" };
        insertValues[1] = updateValues[1];
        writeData(new CsvData(DataEventType.UPDATE, new String[] { getId() }, updateValues),
                insertValues, columns);

        // update a single column
        columns = new String[] { "id", "char_value" };
        insertValues[3] = updateValues[1];
        writeData(new CsvData(DataEventType.UPDATE, new String[] { getId() }, updateValues),
                insertValues, columns);
    }

    @Test
    public void testBinaryColumnTypesForPostgres() throws Exception {
        if (platform instanceof PostgreSqlPlatform) {
            platform.getSqlTemplate().update("drop table if exists test_postgres_binary_types");
            platform.getSqlTemplate().update(
                    "create table test_postgres_binary_types (binary_data oid)");

            String tableName = "test_postgres_binary_types";
            String[] keys = { "binary_data" };
            String[] columns = { "binary_data" };
            String[] values = { "dGVzdCAxIDIgMw==" };

            Table table = buildSourceTable(tableName, keys, columns);
            writeData(new TableCsvData(table, new CsvData(DataEventType.INSERT, values)));

            String result = (String) platform
                    .getSqlTemplate()
                    .queryForObject(
                            "select data from pg_largeobject where loid in (select binary_data from test_postgres_binary_types)",
                            String.class);

            // clean up the object from pg_largeobject, otherwise it becomes
            // abandoned on subsequent runs
            platform.getSqlTemplate().query(
                    "select lo_unlink(binary_data) from test_postgres_binary_types");
            Assert.assertEquals("test 1 2 3", result);
        }
    }

    @Test
    public void testBenchmark() throws Exception {
        Table table = buildSourceTable(TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        int startId = Integer.parseInt(getId()) + 1;
        List<CsvData> datas = new ArrayList<CsvData>();
        for (int i = 0; i < 1600; i++) {
            String[] values = { getNextId(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0",
                    "0", "47", "67.89", "-0.0747663" };
            datas.add(new CsvData(DataEventType.INSERT, values));

        }

        for (int i = startId; i < 1600 + startId; i++) {
            String[] values = { Integer.toString(i) };
            datas.add(new CsvData(DataEventType.DELETE, values, null));
        }

        long startTime = System.currentTimeMillis();
        long statementCount = writeData(new TableCsvData(table, datas));
        double totalSeconds = (System.currentTimeMillis() - startTime) / 1000.0;

        double targetTime = 15.0;
        if (platform instanceof InformixPlatform) {
            targetTime = 20.0;
        }

        Assert.assertEquals(3200, statementCount);

        // TODO: this used to run in 1 second; can we do some optimization?
        Assert.assertTrue("DataLoader running in " + totalSeconds + " is too slow",
                totalSeconds <= targetTime);
    }

    private void massageExpectectedResultsForDialect(String[] values) {
        if (values[5] != null
                && (!(platform instanceof OraclePlatform || platform instanceof MsSqlPlatform))) {
            values[5] = values[5].replaceFirst(" \\d\\d:\\d\\d:\\d\\d\\.?0?", " 00:00:00.0");
        }
        if (values[10] != null) {
            values[10] = values[10].replace(',', '.');
        }
        if (values[10] != null && !(platform instanceof OraclePlatform)) {
            int scale = 17;
            if (platform instanceof MySqlPlatform) {
                scale = 16;
            }
            DecimalFormat df = new DecimalFormat("0.00####################################");
            values[10] = df.format(new BigDecimal(values[10]).setScale(scale, RoundingMode.DOWN));
        }
    }

}
