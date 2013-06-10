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
package org.jumpmind.symmetric.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import junit.framework.Assert;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.symmetric.TestConstants;
import org.jumpmind.symmetric.csv.CsvWriter;
import org.jumpmind.symmetric.ext.NodeGroupTestDataWriterFilter;
import org.jumpmind.symmetric.ext.TestDataWriterFilter;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.writer.Conflict.ResolveConflict;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.impl.DataLoaderService.ConflictNodeGroupLink;
import org.jumpmind.symmetric.transport.MockTransportManager;
import org.jumpmind.symmetric.transport.internal.InternalIncomingTransport;
import org.junit.After;
import org.junit.Test;

abstract public class AbstractDataLoaderServiceTest extends AbstractServiceTest {

    protected final static String TEST_TABLE = "test_dataloader_table";

    protected final static String[] TEST_KEYS = { "id" };

    protected final static String[] TEST_COLUMNS = { "id", "string_value", "string_required_value",
            "char_value", "char_required_value", "date_value", "time_value", "boolean_value",
            "integer_value", "decimal_value", "double_value" };

    protected static int batchId = 10000;

    protected static int sequenceId = 10000;

    protected Node client = new Node(TestConstants.TEST_CLIENT_EXTERNAL_ID, null, null);
    protected Node root = new Node(TestConstants.TEST_ROOT_EXTERNAL_ID, null, null);

    private MockTransportManager transportManager;

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

    @Test
    public void testIncomingBatch() throws Exception {
        String[] insertValues = new String[TEST_COLUMNS.length];
        insertValues[2] = insertValues[4] = "incoming test";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });                
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);

        insertValues[0] = getNextId();
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);

        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertEquals(batch.getStatus(), IncomingBatch.Status.OK, "Wrong status. " + printDatabase());
        assertEquals(batch.getChannelId(), TestConstants.TEST_CHANNEL_ID, "Wrong channel. " + printDatabase());
    }

    @Test
    public void testStatistics() throws Exception {
        Level old = setLoggingLevelForTest(Level.FATAL);
        String[] updateValues = new String[TEST_COLUMNS.length + 1];
        updateValues[0] = updateValues[updateValues.length - 1] = getNextId();
        updateValues[2] = updateValues[4] = "required string";
        String[] insertValues = (String[]) ArrayUtils.subarray(updateValues, 0,
                updateValues.length - 1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });        
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);

        // Update becomes fallback insert
        writer.write(CsvConstants.UPDATE);
        writer.writeRecord(updateValues, true);

        // Insert becomes fallback update
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        // Insert becomes fallback update
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        // Clean insert
        insertValues[0] = getNextId();
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        // Delete affects no rows
        writer.writeRecord(new String[] { CsvConstants.DELETE, getNextId() }, true);
        writer.writeRecord(new String[] { CsvConstants.DELETE, getNextId() }, true);
        writer.writeRecord(new String[] { CsvConstants.DELETE, getNextId() }, true);

        // Failing statement
        insertValues[0] = getNextId();
        insertValues[5] = "i am an invalid date";
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);

        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);
        assertEquals(batch.getStatus(), IncomingBatch.Status.ER, "Wrong status. " + printDatabase());
        assertEquals(batch.getFailedRowNumber(), 8l, "Wrong failed row number. " + batch.getSqlMessage() + ". " + printDatabase());
        assertEquals(batch.getByteCount(), 450l, "Wrong byte count. " + printDatabase());
        assertEquals(batch.getStatementCount(), 8l, "Wrong statement count. " + printDatabase());
        assertEquals(batch.getFallbackInsertCount(), 1l, "Wrong fallback insert count. "
                + printDatabase());
        assertEquals(batch.getFallbackUpdateCount(), 2l, "Wrong fallback update count. "
                + printDatabase());
        assertEquals(batch.getMissingDeleteCount(), 3l, "Wrong missing delete count. "
                + printDatabase());
        setLoggingLevelForTest(old);
    }

    @Test
    public void testUpdateCollision() throws Exception {
        Level old = setLoggingLevelForTest(Level.OFF);
        String[] insertValues = new String[TEST_COLUMNS.length];
        insertValues[0] = getNextId();
        insertValues[2] = insertValues[4] = "inserted row for testUpdateCollision";

        String[] updateValues = new String[TEST_COLUMNS.length];
        updateValues[0] = getId();
        updateValues[TEST_COLUMNS.length - 1] = getNextId();
        updateValues[2] = updateValues[4] = "update will become an insert that violates PK";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });        
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);

        // This insert will be OK
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        // Update becomes fallback insert, and then violate the primary key
        writer.write(CsvConstants.UPDATE);
        writer.writeRecord(updateValues, true);

        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);

        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);

        load(out);

        assertEquals(batch.getStatus(), IncomingBatch.Status.OK, "Wrong status");

        batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);

        assertEquals(batch.getStatus(), IncomingBatch.Status.OK, "Wrong status");

        setLoggingLevelForTest(old);
    }

    @Test
    public void testSqlStatistics() throws Exception {
        Level old = setLoggingLevelForTest(Level.OFF);
        String[] insertValues = new String[TEST_COLUMNS.length];
        insertValues[2] = insertValues[4] = "sql stat test";

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });                
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);

        // Clean insert
        String firstId = getNextId();
        insertValues[0] = firstId;
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        // Clean insert
        String secondId = getNextId();
        insertValues[0] = secondId;
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        String thirdId = getNextId();
        insertValues[0] = thirdId;
        // date column ...
        insertValues[5] = "This is a very long string that will fail upon insert into the database.";
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(insertValues, true);

        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);

        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);
        assertEquals(batch.getStatus(), IncomingBatch.Status.ER, "Wrong status. " + printDatabase());
        assertEquals(batch.getFailedRowNumber(), 3l, "Wrong failed row number. " + printDatabase());
        Assert.assertEquals("Wrong byte count: " + batch.getByteCount() + ". " + printDatabase(), 370,
                batch.getByteCount());
        assertEquals(batch.getStatementCount(), 3l, "Wrong statement count. " + printDatabase());
        assertEquals(batch.getFallbackInsertCount(), 0l, "Wrong fallback insert count. "
                + printDatabase());
        assertEquals(batch.getFallbackUpdateCount(), 0l, "Wrong fallback update count. "
                + printDatabase());
        assertEquals(batch.getMissingDeleteCount(), 0l, "Wrong missing delete count. "
                + printDatabase());
        assertNull(batch.getSqlState(), "Sql state should be null. " + printDatabase());
        assertNotNull(batch.getSqlMessage(), "Sql message should not be null. " + printDatabase());
        setLoggingLevelForTest(old);
    }

    @Test
    public void testSkippingResentBatch() throws Exception {
        String[] values = { getNextId(), "resend string", "resend string not null", "resend char",
                "resend char not null", "2007-01-25 00:00:00.000", "2007-01-25 01:01:01.000", "0", "7",
                "10.10", "0.474" };
        getNextBatchId();
        for (long i = 0; i < 7; i++) {
            batchId--;
            testSimple(CsvConstants.INSERT, values, values);
            assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                    IncomingBatch.Status.OK, "Wrong status");
            IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
            assertNotNull(batch);
            assertEquals(batch.getStatus(), IncomingBatch.Status.OK, "Wrong status");
            assertEquals(batch.getSkipCount(), i);
            assertEquals(batch.getFailedRowNumber(), 0l, "Wrong failed row number");
            assertEquals(batch.getStatementCount(), 1l, "Wrong statement count");
            assertEquals(batch.getFallbackInsertCount(), 0l, "Wrong fallback insert count");
            assertEquals(batch.getFallbackUpdateCount(), 0l, "Wrong fallback update count");
            // pause to make sure we get a different start time on the incoming
            // batch batch
            Thread.sleep(10);
        }
    }

    @Test
    public void testErrorWhileSkip() throws Exception {
        Level old = setLoggingLevelForTest(Level.OFF);
        String[] values = { getNextId(), "string2", "string not null2", "char2", "char not null2",
                "2007-01-02 00:00:00.000", "2007-02-03 04:05:06.000", "0", "47", "67.89", "0.474" };

        testSimple(CsvConstants.INSERT, values, values);
        assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.OK, "Wrong status");
        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);
        assertEquals(batch.getStatus(), IncomingBatch.Status.OK, "Wrong status");
        assertEquals(batch.getFailedRowNumber(), 0l, "Wrong failed row number");
        assertEquals(batch.getStatementCount(), 1l, "Wrong statement count");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });        
        writer.writeRecord(new String[] { CsvConstants.BATCH, getBatchId() });
        writer.write(CsvConstants.KEYS);
        writer.writeRecord(TEST_KEYS);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, getBatchId() });
        writer.close();
        // Pause a moment to guarantee our batch comes back in time order
        Thread.sleep(10);
        load(out);
        assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.OK, "Wrong status");
        setLoggingLevelForTest(old);
    }

    @Test
    public void testDataIntregrityError() throws Exception {
        Level old = setLoggingLevelForTest(Level.OFF);
        String[] values = { getNextId(), "string3", "string not null3", "char3", "char not null3",
                "2007-01-02 00:00:00.000", "2007-02-03 04:05:06.000", "0", "47", "67.89", "0.474" };
        
        ConflictNodeGroupLink conflictSettings = new ConflictNodeGroupLink();
        conflictSettings.setNodeGroupLink(TestConstants.TEST_2_ROOT);
        conflictSettings.setConflictId("dont_fallback");
        conflictSettings.setResolveType(ResolveConflict.MANUAL);
        getSymmetricEngine().getDataLoaderService().save(conflictSettings);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });        
        writer.writeRecord(new String[] { CsvConstants.BATCH, getNextBatchId() });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, getBatchId() });
        writer.close();
        load(out);

        assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.ER, "Wrong status");
        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);
        assertEquals(batch.getStatus(), IncomingBatch.Status.ER, "Wrong status");
        assertEquals(batch.getFailedRowNumber(), 2l, "Wrong failed row number");
        assertEquals(batch.getStatementCount(), 2l, "Wrong statement count");

        load(out);
        assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.ER, "Wrong status");
        batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);
        assertEquals(batch.getStatus(), IncomingBatch.Status.ER, "Wrong status");
        assertEquals(batch.getFailedRowNumber(), 2l, "Wrong failed row number");
        assertEquals(batch.getStatementCount(), 2l, "Wrong statement count");

        getSymmetricEngine().getDataLoaderService().delete(conflictSettings);
        setLoggingLevelForTest(old);
    }

    @Test
    public void testErrorWhileParsing() throws Exception {
        Level old = setLoggingLevelForTest(Level.OFF);
        String[] values = { getNextId(), "should not reach database", "string not null", "char",
                "char not null", "2007-01-02", "2007-02-03 04:05:06.000", "0", "47", "67.89", "0.474" };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });                
        String nextBatchId = getNextBatchId();
        writer.write("UnknownTokenOutsideBatch");                
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writer.writeRecord(new String[] { CsvConstants.TABLE, TEST_TABLE });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);
        assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID), null,
                "Wrong status");
        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNull(batch);
        setLoggingLevelForTest(old);
    }

    @Test
    public void testErrorThenSuccessBatch() throws Exception {
        Logger.getLogger(AbstractDataLoaderServiceTest.class).warn("testErrorThenSuccessBatch");
        Level old = setLoggingLevelForTest(Level.OFF);
        String[] values = { getNextId(),
                "This string is too large and will cause the statement to fail",
                "string not null2", "char2", "char not null2", "not a date",
                "2007-02-03 04:05:06.000", "0", "47", "123456789.00", "0.474" };
        getNextBatchId();
        int retries = 3;
        for (int i = 0; i < retries; i++) {
            batchId--;
            testSimple(CsvConstants.INSERT, values, null);
            assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                    IncomingBatch.Status.ER, "Wrong status");
            IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
            assertNotNull(batch);
            assertEquals(batch.getStatus(), IncomingBatch.Status.ER, "Wrong status. "
                    + printDatabase());
            assertEquals(batch.getFailedRowNumber(), 1l, "Wrong failed row number. "
                    + printDatabase());
            assertEquals(batch.getStatementCount(), 1l, "Wrong statement count. " + printDatabase());
            // pause to make sure we get a different start time on the incoming
            // batch batch
            Thread.sleep(10);
        }

        batchId--;
        values[1] = "A smaller string that will succeed";
        values[5] = "2007-01-02 00:00:00.000";
        values[9] = "67.89";
        testSimple(CsvConstants.INSERT, values, values);
        assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.OK, "Wrong status. " + printDatabase());
        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId,
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        assertNotNull(batch);
        assertEquals(batch.getStatus(), IncomingBatch.Status.OK, "Wrong status. " + printDatabase());
        assertEquals(batch.getFailedRowNumber(), 0l, "Wrong failed row number. " + printDatabase());
        assertEquals(batch.getStatementCount(), 1l, "Wrong statement count. " + printDatabase());
        setLoggingLevelForTest(old);
    }

    @Test
    public void testMultipleBatch() throws Exception {
        Level old = setLoggingLevelForTest(Level.OFF);
        String[] values = { getNextId(), "string", "string not null2", "char2", "char not null2",
                "2007-01-02 00:00:00.000", "2007-02-03 04:05:06.000", "0", "47", "67.89", "0.474" };
        String[] values2 = { getNextId(),
                "This string is too large and will cause the statement to fail",
                "string not null2", "char2", "char not null2", "Not a date",
                "2007-02-03 04:05:06.000", "0", "47", "123456789.00", "0.474" };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });                
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });

        String nextBatchId2 = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId2 });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values2, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId2 });

        writer.close();
        load(out);
        assertTestTableEquals(values[0], values);
        assertTestTableEquals(values2[0], null);

        assertEquals(
                findIncomingBatchStatus(Integer.parseInt(nextBatchId),
                        TestConstants.TEST_CLIENT_EXTERNAL_ID), IncomingBatch.Status.OK,
                "Wrong status. " + printDatabase());
        assertEquals(
                findIncomingBatchStatus(Integer.parseInt(nextBatchId2),
                        TestConstants.TEST_CLIENT_EXTERNAL_ID), IncomingBatch.Status.ER,
                "Wrong status. " + printDatabase());
        setLoggingLevelForTest(old);
    }

    public void testSimple(String dmlType, String[] values, String[] expectedValues)
            throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID,
                TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.CHANNEL,
                TestConstants.TEST_CHANNEL_ID });                
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

    protected void load(ByteArrayOutputStream out) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        getTransportManager().setIncomingTransport(new InternalIncomingTransport(in));
        getDataLoaderService().loadDataFromPull(client);
    }

    protected IncomingBatch.Status findIncomingBatchStatus(int batchId, String nodeId) {
        IncomingBatch batch = getIncomingBatchService().findIncomingBatch(batchId, nodeId);
        IncomingBatch.Status status = null;
        if (batch != null) {
            status = batch.getStatus();
        }
        return status;
    }

    @Test
    public void testRegisteredDataWriterFilter() {
        TestDataWriterFilter registeredFilter = getSymmetricEngine().getExtensionPointManager()
                .getExtensionPoint("registeredDataFilter");
        assertTrue(registeredFilter.getNumberOfTimesCalled() > 0);

        NodeGroupTestDataWriterFilter registeredNodeGroupFilter = getSymmetricEngine()
                .getExtensionPointManager().getExtensionPoint("registeredNodeGroupTestDataFilter");
        assertTrue(registeredNodeGroupFilter.getNumberOfTimesCalled() > 0);
    }

    protected CsvWriter getWriter(OutputStream out) {
        CsvWriter writer = new CsvWriter(new OutputStreamWriter(out), ',');
        writer.setEscapeMode(CsvWriter.ESCAPE_MODE_BACKSLASH);
        return writer;
    }

    protected MockTransportManager getTransportManager() {
        if (transportManager == null) {
            transportManager = new MockTransportManager();
        }
        return transportManager;
    }

    protected void writeTable(CsvWriter writer, String tableName, String[] keys, String[] columns)
            throws IOException {
        writer.writeRecord(new String[] { "table", tableName });
        writer.write("keys");
        writer.writeRecord(keys);
        writer.write("columns");
        writer.writeRecord(columns);
    }

    protected void assertTestTableEquals(String testTableId, String[] expectedValues) {
        String sql = "select " + getSelect(TEST_COLUMNS) + " from " + TEST_TABLE + " where "
                + getWhere(TEST_KEYS);
        Map<String, Object> results = getSqlTemplate().queryForMap(sql, new Object[] { new Long(testTableId) });
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
                char decimal = ((DecimalFormat) DecimalFormat.getInstance())
                        .getDecimalFormatSymbols().getDecimalSeparator();
                if ((resultObj instanceof Double || resultObj instanceof BigDecimal) && expected[i].indexOf(decimal) != -1) {
                    DecimalFormat df = new DecimalFormat("0.00####################################");
                    resultValue = df.format(resultObj);
                } else if (resultObj instanceof Date) {
                    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.000");
                    resultValue = df.format(resultObj);
                } else if (resultObj instanceof Boolean) {
                    resultValue = ((Boolean) resultObj) ? "1" : "0";
                } else if (resultObj instanceof Double) {
                    resultValue = resultObj.toString();
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
        if (isRequired
                && (value == null || (value.equals("") && getDbDialect().getPlatform()
                        .getDatabaseInfo().isEmptyStringNulled()))) {
            return AbstractDatabasePlatform.REQUIRED_FIELD_NULL_SUBSTITUTE;
        } else if (value != null && value.equals("")
                && getDbDialect().getPlatform().getDatabaseInfo().isEmptyStringNulled()) {
            return null;
        }
        return value;
    }

    protected String translateExpectedCharString(String value, int size, boolean isRequired) {
        if (isRequired && value == null) {
            value = AbstractDatabasePlatform.REQUIRED_FIELD_NULL_SUBSTITUTE;
        }
        if (value != null
                && ((StringUtils.isBlank(value) && getDbDialect().getPlatform().getDatabaseInfo()
                        .isBlankCharColumnSpacePadded()) || (StringUtils.isNotBlank(value) && getDbDialect()
                        .getPlatform().getDatabaseInfo().isNonBlankCharColumnSpacePadded()))) {
            return StringUtils.rightPad(value, size);
        } else if (value != null
                && getDbDialect().getPlatform().getDatabaseInfo().isCharColumnSpaceTrimmed()) {
            return value.replaceFirst(" *$", "");
        }
        return value;
    }

    protected IDataLoaderService getDataLoaderService() {
        DataLoaderService dataLoaderService = (DataLoaderService) getSymmetricEngine()
                .getDataLoaderService();
        dataLoaderService.setTransportManager(transportManager);
        return dataLoaderService;
    }
    
    @After
    public void cleanup() {
        getSymmetricEngine().getStagingManager().clean();
    }

}
