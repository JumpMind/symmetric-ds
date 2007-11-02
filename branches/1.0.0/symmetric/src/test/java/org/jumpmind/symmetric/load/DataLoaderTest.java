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
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.transport.TransportUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.csvreader.CsvWriter;

public class DataLoaderTest extends AbstractDataLoaderTest {

    protected static final String INSERT_EXISTING_ID = "1";

    protected static final String DELETE_EXISTING_ID = "5";

    protected static final String DELETE_NOT_EXISTING_ID = "6";

    protected static final String UPDATE_NOT_EXISTING_ID = "7";

    @Test(groups="continuous")
    public void testInsertExisting() throws Exception {
        String[] values = { INSERT_EXISTING_ID, "string2", "string not null2", "char2", "char not null2",
                "2007-01-02", "2007-02-03 04:05:06.0", "0", "47", "67.89" };
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test(groups="continuous")
    public void testUpdateNotExisting() throws Exception {
        String[] values = { UPDATE_NOT_EXISTING_ID, "it's /a/  string", "it's  -not-  null",
                "You're a \"character\"", "Where are you?", "2007-12-31", "2007-12-31 23:59:59.0", "1", "13",
                "9.95", UPDATE_NOT_EXISTING_ID };
        String[] expectedValues = (String[]) ArrayUtils.subarray(values, 0, values.length - 1);
        testSimple(CsvConstants.UPDATE, values, expectedValues);
    }

    @Test(groups="continuous")
    public void testStringQuotes() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[1] = "It's \"quoted,\" with a comma";
        values[2] = "two 'ticks'";
        values[3] = "One quote\"";
        values[4] = "One comma,";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test(groups="continuous")
    public void testStringSpaces() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[1] = "  two spaces before";
        values[2] = "two spaces after  ";
        values[3] = " one space before";
        values[4] = "one space after ";
        String[] expectedValues = values.clone();
        expectedValues[4] = translateExpectedCharString(expectedValues[4], 50);
        testSimple(CsvConstants.INSERT, values, expectedValues);
    }

    @Test(groups="continuous")
    public void testStringOneSpace() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[2] = values[4] = " ";
        String[] expectedValues = values.clone();
        expectedValues[4] = translateExpectedCharString(expectedValues[4], 50);
        testSimple(CsvConstants.INSERT, values, expectedValues);
    }

    @Test(groups="continuous")
    public void testStringEmpty() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[1] = values[2] = values[3] = values[4] = "";
        String[] expectedValues = values.clone();
        expectedValues[2] = translateExpectedString(expectedValues[2]);
        expectedValues[4] = translateExpectedCharString(expectedValues[4], 50);
        testSimple(CsvConstants.INSERT, values, expectedValues);
    }

    @Test(groups="continuous")
    public void testStringNull() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        String[] expectedValues = values.clone();
        expectedValues[2] = expectedValues[4] = TableTemplate.REQUIRED_FIELD_NULL_SUBSTITUTE;
        expectedValues[4] = translateExpectedCharString(expectedValues[4], 50);
        testSimple(CsvConstants.INSERT, values, expectedValues);
    }

    @Test(groups="continuous")
    public void testStringBackslash() throws Exception {
        String[] values = new String[10];
        values[0] = getNextId();
        values[1] = "Here's a \\, a (backslash)";
        values[2] = "Fix TODO";
        // TODO: Fix backslashing alphanumeric 
        //values[2] = "\\a\\b\\c\\ \\1\\2\\3";
        values[3] = "Tick quote \\'\\\"";
        values[4] = "Comma quote \\,\\\"";
        testSimple(CsvConstants.INSERT, values, values);
    }

    @Test(groups="continuous")
    public void testDeleteExisting() throws Exception {
        testSimple(CsvConstants.DELETE, new String[] { DELETE_EXISTING_ID }, null);
    }

    @Test(groups="continuous")
    public void testDeleteNotExisting() throws Exception {
        testSimple(CsvConstants.DELETE, new String[] { DELETE_NOT_EXISTING_ID }, null);
    }

    @Test(groups="continuous")
    public void testColumnNotExisting() throws Exception {
        String[] columns = (String[]) ArrayUtils.add(TEST_COLUMNS, "Unknown_Column");
        String[] values = { getNextId(), "testColumnNotExisting", "string not null", "char", "char not null",
                "2007-01-02", "2007-02-03 04:05:06.0", "0", "47", "67.89", "i do not exist!" };
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
        assertTestTableEquals(values[0], expectedValues);    
    }

    @Test(groups="continuous")
    public void testTableNotExisting() throws Exception {
        String tableName = "UnknownTable";
        String[] keys = { "id" };
        String[] columns= { "id", "name" };
        String[] badValues = { "1", "testTableNotExisting" };
        String[] values = { getNextId(), "testTableNotExisting", "This row should load", "char",
                "char not null", "2007-01-02", "2007-02-03 04:05:06.0", "0", "0", "12.10"};

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
        assertTestTableEquals(values[0], values);
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
        Assert.assertTrue(totalSeconds <= 3.0, "DataLoader running in " + totalSeconds + " is too slow");
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
        return (IDataLoader) getBeanFactory().getBean(Constants.DATALOADER);
    }

}
