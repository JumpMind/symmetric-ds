/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.load;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math.random.RandomDataImpl;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.db.mssql.MsSqlDbDialect;
import org.jumpmind.symmetric.db.oracle.OracleDbDialect;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.junit.Assert;
import org.junit.Test;

import com.csvreader.CsvWriter;

public class DataLoaderTest extends AbstractDataLoaderTest {

    public DataLoaderTest() throws Exception {
    }

    public DataLoaderTest(String db) {
        super(db);
    }

    @Test
    public void testInsertExisting() throws Exception {
        String[] values = { getNextId(), "string2", "string not null2", "char2", "char not null2",
                "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0", "47", "67.89" };
        massageExpectectedResultsForDialect(values);
        testSimple(CsvConstants.INSERT, values, values);

        values[1] = "insert fallback to update";
        massageExpectectedResultsForDialect(values);
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testUpdateNotExisting() throws Exception {
        String id = getNextId();
        String[] values = { id, "it's /a/  string", "it's  -not-  null", "You're a \"character\"", "Where are you?",
                "2007-12-31 02:33:45.0", "2007-12-31 23:59:59.0", "1", "13", "9.95", id };
        String[] expectedValues = (String[]) ArrayUtils.subarray(values, 0, values.length - 1);
        massageExpectectedResultsForDialect(expectedValues);
        testSimple(CsvConstants.UPDATE, values, expectedValues);
    }

    @Test
    public void testStringQuotes() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[1] = "It's \"quoted,\" with a comma";
        values[2] = "two 'ticks'";
        values[3] = "One quote\"";
        values[4] = "One comma,";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringSpaces() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[1] = "  two spaces before";
        values[2] = "two spaces after  ";
        values[3] = " one space before";
        values[4] = "one space after ";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringOneSpace() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[2] = values[4] = " ";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringEmpty() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[1] = values[2] = values[3] = values[4] = "";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringNull() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test
    public void testStringBackslash() throws Exception {
        String[] values = new String[10];
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
                "2007-01-02 03:20:10.0", "2007-02-03 04:05:06.0", "0", "47", "67.89" };
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
                "2007-01-02 00:00:00.0", "2007-02-03 04:05:06.0", "0", "47", "67.89", "i do not exist!" };
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
                "2007-01-02 00:00:00.0", "2007-02-03 04:05:06.0", "0", "0", "12.10" };

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
        if (!(getDbDialect() instanceof OracleDbDialect || getDbDialect() instanceof MsSqlDbDialect)) {
            values[5] = values[5].replaceFirst(" \\d\\d:\\d\\d:\\d\\d\\.?0?", " 00:00:00.0");
        }
    }

    @Test
    public void testBenchmark() throws Exception {
        ZipInputStream in = new ZipInputStream(getClass().getResourceAsStream("/test-data-loader-benchmark.zip"));
        in.getNextEntry();
        long startTime = System.currentTimeMillis();
        IDataLoader dataLoader = getDataLoader();
        dataLoader.open(TransportUtils.toReader(in));
        while (dataLoader.hasNext()) {
            dataLoader.load();
        }
        dataLoader.close();
        double totalSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
        // TODO: this used to run in 1 second; can we do some optimization?
        Assert.assertTrue("DataLoader running in " + totalSeconds + " is too slow", totalSeconds <= 12.0);
    }

    protected void load(ByteArrayOutputStream out) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        IDataLoader dataLoader = getDataLoader();
        dataLoader.open(TransportUtils.toReader(in));
        while (dataLoader.hasNext()) {
            dataLoader.load();
        }
        dataLoader.close();
    }

    protected IDataLoader getDataLoader() {
        return (IDataLoader) find(Constants.DATALOADER);
    }

}
