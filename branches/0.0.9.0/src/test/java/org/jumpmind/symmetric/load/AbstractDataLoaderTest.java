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
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.springframework.dao.EmptyResultDataAccessException;
import org.testng.Assert;

import com.csvreader.CsvWriter;

public abstract class AbstractDataLoaderTest extends AbstractTest {

    protected final static String TEST_TABLE = "test_dataloader_table";
    
    protected final static String[] TEST_KEYS = { "id" };
    
    protected final static String[] TEST_COLUMNS = { "id", "string_value", "string_required_value",
            "char_value", "char_required_value", "date_value", "time_value", "boolean_value",
            "integer_value", "decimal_value" };
    
    protected static int batchId = 0;
    
    protected static int sequenceId = 9;

    @SuppressWarnings("unchecked")
    public void testSimple(String dmlType, String[] values, String[] expectedValues) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
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

    protected void writeTable(CsvWriter writer, String tableName, String[] keys, String[] columns)
            throws IOException {
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
            results = getJdbcTemplate().queryForMap(sql, new Object[] { testTableId });
        } catch (EmptyResultDataAccessException e) {
        }
        assertEquals(TEST_COLUMNS, expectedValues, results);
    }
    
    protected void assertEquals(String[] name, String[] expected, Map<String, Object> results) {
        if (expected == null) {
            Assert.assertNull(results, "Expected empty results");
        } else {
            Assert.assertNotNull(results, "Expected non-empty results");
            for (int i = 0; i < expected.length; i++) {
                String result = results.get(name[i]) != null ? results.get(name[i]).toString() : null;
                Assert.assertEquals(result, expected[i], name[i]);
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

    protected String translateExpectedString(String value) {
        if (value != null && value.equals("") && getDbDialect().isEmptyStringNulled()) {
            return null;
        }
        return value;
    }

    protected String translateExpectedCharString(String value, int size) {
        value = translateExpectedString(value);
        if (value != null && getDbDialect().isCharSpacePadded()) {
            return StringUtils.rightPad(value, size);
        } else if (value != null && getDbDialect().isCharSpaceTrimmed()) {
            return value.trim();
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
