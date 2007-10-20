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

package org.jumpmind.symmetric.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.load.AbstractDataLoaderTest;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.transport.internal.InternalIncomingTransport;
import org.jumpmind.symmetric.transport.mock.MockTransportManager;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.csvreader.CsvWriter;

public class DataLoaderServiceTest extends AbstractDataLoaderTest {

    protected IDataLoaderService dataLoaderService;

    protected IIncomingBatchService incomingBatchService;

    protected MockTransportManager transportManager;

    protected Node client;

    @BeforeTest(groups = "continuous")
    protected void setUp() {
        dataLoaderService = (IDataLoaderService) getBeanFactory().getBean(Constants.DATALOADER_SERVICE);
        incomingBatchService = (IIncomingBatchService) getBeanFactory().getBean(
                Constants.INCOMING_BATCH_SERVICE);
        transportManager = new MockTransportManager();
        dataLoaderService.setTransportManager(transportManager);
        client = new Node();
        client.setNodeId(TestConstants.TEST_CLIENT_EXTERNAL_ID);
    }

    @Test(groups = "continuous")
    public void testStatistics() throws Exception {
        String[] updateValues = new String[11];
        updateValues[0] = updateValues[10] = getNextId();
        updateValues[2] = updateValues[4] = "required string";
        String[] insertValues = (String[]) ArrayUtils.subarray(updateValues, 0, updateValues.length - 1);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });

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

        List<IncomingBatchHistory> list = incomingBatchService.findIncomingBatchHistory(batchId + "",
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertEquals(list.size(), 1, "Wrong number of history");
        IncomingBatchHistory history = list.get(0);
        Assert.assertEquals(history.getStatus(), IncomingBatchHistory.Status.ER, "Wrong status");
        Assert.assertNotNull(history.getStartTime(), "Start time cannot be null");
        Assert.assertNotNull(history.getEndTime(), "End time cannot be null");
        Assert.assertEquals(history.getFailedRowNumber(), 8, "Wrong failed row number");
        Assert.assertEquals(history.getStatementCount(), 8, "Wrong statement count");
        Assert.assertEquals(history.getFallbackInsertCount(), 1, "Wrong fallback insert count");
        Assert.assertEquals(history.getFallbackUpdateCount(), 2, "Wrong fallback update count");
        Assert.assertEquals(history.getMissingDeleteCount(), 3, "Wrong missing delete count");
    }

    @Test(groups = "continuous")
    public void testSkippingResentBatch() throws Exception {
        String[] values = { getNextId(), "resend string", "resend string not null", "resend char",
                "resend char not null", "2007-01-25", "2007-01-25 01:01:01.0", "0", "7", "10.10" };
        getNextBatchId();
        for (int i = 0; i < 7; i++) {
            batchId--;
            testSimple(CsvConstants.INSERT, values, values);
            IncomingBatchHistory.Status expectedStatus = IncomingBatchHistory.Status.OK;
            long expectedCount = 1;
            if (i > 0) {
                expectedStatus = IncomingBatchHistory.Status.SK;
                expectedCount = 0;
            }
            Assert.assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                    IncomingBatch.Status.OK, "Wrong status");
            List<IncomingBatchHistory> list = incomingBatchService.findIncomingBatchHistory(batchId + "",
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
            Assert.assertEquals(list.size(), i + 1, "Wrong number of history");
            IncomingBatchHistory history = list.get(i);
            Assert.assertEquals(history.getStatus(), expectedStatus, "Wrong status");
            Assert.assertEquals(history.getFailedRowNumber(), 0, "Wrong failed row number");
            Assert.assertEquals(history.getStatementCount(), expectedCount, "Wrong statement count");
            Assert.assertEquals(history.getFallbackInsertCount(), 0, "Wrong fallback insert count");
            Assert.assertEquals(history.getFallbackUpdateCount(), 0, "Wrong fallback update count");
        }
    }

    @Test(groups = "continuous")
    public void testErrorWhileSkip() throws Exception {
        String[] values = { getNextId(), "string2", "string not null2", "char2", "char not null2",
                "2007-01-02", "2007-02-03 04:05:06.0", "0", "47", "67.89" };

        testSimple(CsvConstants.INSERT, values, values);
        Assert.assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.OK, "Wrong status");
        List<IncomingBatchHistory> list = incomingBatchService.findIncomingBatchHistory(batchId + "",
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertEquals(list.size(), 1, "Wrong number of history");
        IncomingBatchHistory history = list.get(0);
        Assert.assertEquals(history.getStatus(), IncomingBatchHistory.Status.OK, "Wrong status");
        Assert.assertEquals(history.getFailedRowNumber(), 0, "Wrong failed row number");
        Assert.assertEquals(history.getStatementCount(), 1, "Wrong statement count");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.writeRecord(new String[] { CsvConstants.BATCH, getBatchId() });
        writer.writeRecord(new String[] { CsvConstants.TABLE, TEST_TABLE });
        writer.write("UnknownTokenWithinBatch");
        writer.writeRecord(new String[] { CsvConstants.COMMIT, getBatchId() });
        writer.close();
        load(out);
        Assert.assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.OK, "Wrong status");
        list = incomingBatchService.findIncomingBatchHistory(batchId + "", TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertEquals(list.size(), 2, "Wrong number of history");
        history = list.get(1);
        Assert.assertEquals(history.getStatus(), IncomingBatchHistory.Status.ER, "Wrong status");
        Assert.assertEquals(history.getFailedRowNumber(), 2, "Wrong failed row number");
        Assert.assertEquals(history.getStatementCount(), 0, "Wrong statement count");
    }

    @Test(groups = "continuous")
    public void testErrorWhileParsing() throws Exception {
        String[] values = { getNextId(), "should not reach database", "string not null", "char",
                "char not null", "2007-01-02", "2007-02-03 04:05:06.0", "0", "47", "67.89" };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writer.write("UnknownTokenOutsideBatch");
        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writer.writeRecord(new String[] { CsvConstants.TABLE, TEST_TABLE });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });
        writer.close();
        load(out);
        Assert.assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID), null,
                "Wrong status");
        List<IncomingBatchHistory> list = incomingBatchService.findIncomingBatchHistory(batchId + "",
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertEquals(list.size(), 0, "Wrong number of history");
    }

    @Test(groups = "continuous")
    public void testErrorThenSuccessBatch() throws Exception {
        String[] values = { getNextId(), "This string is too large and will cause the statement to fail",
                "string not null2", "char2", "char not null2", "2007-01-02", "2007-02-03 04:05:06.0", "0",
                "47", "67.89" };
        getNextBatchId();
        int retries = 3;
        for (int i = 0; i < retries; i++) {
            batchId--;
            testSimple(CsvConstants.INSERT, values, null);
            Assert.assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                    IncomingBatch.Status.ER, "Wrong status");
            List<IncomingBatchHistory> list = incomingBatchService.findIncomingBatchHistory(batchId + "",
                    TestConstants.TEST_CLIENT_EXTERNAL_ID);
            Assert.assertEquals(list.size(), i + 1, "Wrong number of history");
            IncomingBatchHistory history = list.get(i);
            Assert.assertEquals(history.getStatus(), IncomingBatchHistory.Status.ER, "Wrong status");
            Assert.assertEquals(history.getFailedRowNumber(), 1, "Wrong failed row number");
            Assert.assertEquals(history.getStatementCount(), 1, "Wrong statement count");
        }

        batchId--;
        values[1] = "A smaller string that will succeed";
        testSimple(CsvConstants.INSERT, values, values);
        Assert.assertEquals(findIncomingBatchStatus(batchId, TestConstants.TEST_CLIENT_EXTERNAL_ID),
                IncomingBatch.Status.OK, "Wrong status");
        List<IncomingBatchHistory> list = incomingBatchService.findIncomingBatchHistory(batchId + "",
                TestConstants.TEST_CLIENT_EXTERNAL_ID);
        Assert.assertEquals(list.size(), retries + 1, "Wrong number of history");
        IncomingBatchHistory history = list.get(retries);
        Assert.assertEquals(history.getStatus(), IncomingBatchHistory.Status.OK, "Wrong status");
        Assert.assertEquals(history.getFailedRowNumber(), 0, "Wrong failed row number");
        Assert.assertEquals(history.getStatementCount(), 1, "Wrong statement count");
    }

    @Test(groups = "continuous")
    public void testMultipleBatch() throws Exception {
        String[] values = { getNextId(), "string", "string not null2", "char2", "char not null2",
                "2007-01-02", "2007-02-03 04:05:06.0", "0", "47", "67.89" };
        String[] values2 = { getNextId(), "This string is too large and will cause the statement to fail",
                "string not null2", "char2", "char not null2", "2007-01-02", "2007-02-03 04:05:06.0", "0",
                "47", "67.89" };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CsvWriter writer = getWriter(out);
        writer.writeRecord(new String[] { CsvConstants.NODEID, TestConstants.TEST_CLIENT_EXTERNAL_ID });
        writeTable(writer, TEST_TABLE, TEST_KEYS, TEST_COLUMNS);

        String nextBatchId = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId });

        String nextBatchId2 = getNextBatchId();
        writer.writeRecord(new String[] { CsvConstants.BATCH, nextBatchId2 });
        writer.write(CsvConstants.INSERT);
        writer.writeRecord(values2, true);
        writer.writeRecord(new String[] { CsvConstants.COMMIT, nextBatchId2 });

        writer.close();
        load(out);
        assertTestTableEquals(values[0], values);
        assertTestTableEquals(values2[0], null);

        Assert.assertEquals(findIncomingBatchStatus(Integer.parseInt(nextBatchId),
                TestConstants.TEST_CLIENT_EXTERNAL_ID), IncomingBatch.Status.OK, "Wrong status");
        Assert.assertEquals(findIncomingBatchStatus(Integer.parseInt(nextBatchId2),
                TestConstants.TEST_CLIENT_EXTERNAL_ID), IncomingBatch.Status.ER, "Wrong status");
    }

    protected void load(ByteArrayOutputStream out) throws Exception {
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        transportManager.setIncomingTransport(new InternalIncomingTransport(in));
        dataLoaderService.loadData(client, null);
    }

    protected IncomingBatch.Status findIncomingBatchStatus(int batchId, String clientId) {
        IncomingBatch batch = incomingBatchService.findIncomingBatch(batchId + "", clientId);
        IncomingBatch.Status status = null;
        if (batch != null) {
            status = batch.getStatus();
        }
        return status;
    }

}
