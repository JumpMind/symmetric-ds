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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.transport.mock.MockTransportManager;
import org.junit.Assert;
import org.springframework.dao.EmptyResultDataAccessException;

import com.csvreader.CsvWriter;

public abstract class AbstractDataLoaderTest extends AbstractDatabaseTest {

    protected final static String TEST_TABLE = "test_dataloader_table";

    protected final static String[] TEST_KEYS = { "id" };

    protected final static String[] TEST_COLUMNS = { "id", "string_value", "string_required_value", "char_value",
            "char_required_value", "date_value", "time_value", "boolean_value", "integer_value", "decimal_value" };

    protected static int batchId = 10000;

    protected static int sequenceId = 10000;

    private IDataLoaderService dataLoaderService;

    private IIncomingBatchService incomingBatchService;

    private MockTransportManager transportManager;

    public AbstractDataLoaderTest() throws Exception {
    }

    public AbstractDataLoaderTest(String dbType) {
        super(dbType);
    }

    protected MockTransportManager getTransportManager() {
        if (transportManager == null) {
            transportManager = new MockTransportManager();
        }
        return transportManager;
    }

    protected IIncomingBatchService getIncomingBatchService() {
        if (incomingBatchService == null) {
            incomingBatchService = (IIncomingBatchService) find(Constants.INCOMING_BATCH_SERVICE);
        }
        return incomingBatchService;
    }

    protected IDataLoaderService getDataLoaderService() {
        if (dataLoaderService == null) {
            dataLoaderService = (IDataLoaderService) find(Constants.DATALOADER_SERVICE);
            dataLoaderService.setTransportManager(transportManager);
        }
        return dataLoaderService;
    }

    protected String printDatabase() {
        return " The database we are testing against is " + database + ".";
    }

    @SuppressWarnings("unchecked")
    public void testSimple(String dmlType, String[] values, String[] expectedValues) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(dmlType);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);
        assertTestTableEquals(values[0], expectedValues);
    }

    protected CsvWriter getWriter(OutputStream out) {
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        return writer;
    }

    protected abstract void load(ByteArrayOutputStream out) throws Exception;

    protected void writeTable(CsvWriter writer, String tableName, String[] keys, String[] columns) throws IOException {
        writer.writeRecord(new String[] { "table", tableName });
        writer.write("keys");
        writer.writeRecord(keys);
        writer.write("columns");
        writer.writeRecord(columns);
    }

    @SuppressWarnings("unchecked")
    protected void assertTestTableEquals(String testTableId, String[] expectedValues) {
        String sql = "select " + getSelect(TEST_COLUMNS) + " from " + TEST_TABLE + " where " + getWhere(TEST_KEYS);
        Map<String, Object> results = null;
        try {
            results = getJdbcTemplate().queryForMap(sql, new Object[] { new Long(testTableId) });
        } catch (EmptyResultDataAccessException e) {
        }
        if (expectedValues != null) {
            expectedValues[1] = translateExpectedString(expectedValues[1], false);
            expectedValues[2] = translateExpectedString(expectedValues[2], true);
            expectedValues[3] = translateExpectedCharString(expectedValues[3], 50, false);
            expectedValues[4] = translateExpectedCharString(expectedValues[4], 50, true);
        }
        assertEquals(TEST_COLUMNS, expectedValues, results);
    }

    protected void assertEquals(String[] name, String[] expected, Map<String, Object> results) {
        if (expected == null) {
            Assert.assertNull("Expected empty results. " + printDatabase(), results);
        } else {
            Assert.assertNotNull("Expected non-empty results. " + printDatabase(), results);
            for (int i = 0; i < expected.length; i++) {
                Object resultObj = results.get(name[i]);
                String resultValue = null;
                if (resultObj instanceof BigDecimal && expected[i].indexOf(".") != -1) {
                    DecimalFormat df = new DecimalFormat("#.00");
                    resultValue = df.format(resultObj);
                } else if (resultObj instanceof Date) {
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
                    resultValue = df.format(resultObj);
                } else if (resultObj instanceof Boolean) {
                    resultValue = ((Boolean) resultObj) ? "1" : "0";
                } else if (resultObj != null) {
                    resultValue = resultObj.toString();
                }

                Assert.assertEquals(name[i] + ". " + printDatabase(), expected[i], resultValue);
            }
        }
    }

    protected String getSelect(String[] columns) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            str.append(columns[i]).append(i + 1 < columns.length ? ", " : "");
        }
        return str.toString();
    }

    protected String getWhere(String[] columns) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            str.append(columns[i]).append(" = ?").append(i + 1 < columns.length ? "," : "");
        }
        return str.toString();
    }

    protected String translateExpectedString(String value, boolean isRequired) {
        if (isRequired && (value == null || (value.equals("") && getDbDialect().isEmptyStringNulled()))) {
            return TableTemplate.REQUIRED_FIELD_NULL_SUBSTITUTE;
        } else if (value != null && value.equals("") && getDbDialect().isEmptyStringNulled()) {
            return null;
        }
        return value;
    }

    protected String translateExpectedCharString(String value, int size, boolean isRequired) {
        if (isRequired && value == null) {
            value = TableTemplate.REQUIRED_FIELD_NULL_SUBSTITUTE;
        }
        if (value != null && getDbDialect().isCharSpacePadded()) {
            return StringUtils.rightPad(value, size);
        } else if (value != null && getDbDialect().isCharSpaceTrimmed()) {
            return value.replaceFirst(" *$", "");
        }
        return value;
    }

    protected synchronized String getNextBatchId() {
        return Integer.toString(++batchId);
    }

    protected synchronized String getBatchId() {
        return Integer.toString(batchId);
    }

    protected synchronized String getNextId() {
        return Integer.toString(++sequenceId);
    }

    protected synchronized String getId() {
        return Integer.toString(sequenceId);
    }

}
