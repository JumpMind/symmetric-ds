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


package org.jumpmind.symmetric.extract;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;

/**
 * 
 */
public class DataExtractorTest extends AbstractDatabaseTest {
    
    private static final String CONTEXT_NAME = "extractorContext";

    private IDataExtractor dataExtractor;

    private IParameterService parameterService;

    private IDbDialect dbDialect;

    private final TestData TD1 = new TestData(999, "foo", "\"abc\", 123, \"xyz\"", "328", "basket_id",
            "mango, watermellon, grape");

    private final TestData TD2 = new TestData(998, "foo", "\"www\", 888, \"ghi\"", "6578", "basket_id",
            "mango, watermellon, grape");

    private final TestData TD3 = new TestData(997, "foo", "\"monday\", 879, \"ggg\"", "6502", "basket_id",
            "grape, tomato, cucumber");

    private final TestData TD4 = new TestData(997, "bar", "\"monday\", 879, \"ggg\"", "6502", "basket_id",
            "grape, tomato, cucumber");
    
    

    public DataExtractorTest() throws Exception {
        super();
    }

    @Before
    public void setUp() {
        dataExtractor = (IDataExtractor) find(Constants.DATA_EXTRACTOR);
        parameterService = (IParameterService) find(Constants.PARAMETER_SERVICE);
        dbDialect = (IDbDialect) find(Constants.DB_DIALECT);
    }

    @Test
    public void basicTest() {
        TriggerHistory hist = makeTableSynchistoryId(TD1.table, TD1.keyColumns, TD1.columns);

        try {
            DataExtractorContext context = (DataExtractorContext) find(CONTEXT_NAME);

            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            long batchId = 998877;
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);

            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, TD1.table, new Date(), hist, TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);
            dataExtractor.commit(batch, writer);

            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(parameterService.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.batchEnd(batchId);

            writer.flush();
            Assert.assertEquals(stringWriter.toString(), em.toString());
        } catch (IOException e) {
            Assert.fail("BasicTeset failed");
        }

        reset();
    }

    @Test
    public void biggerTest() {
        TriggerHistory history = makeTableSynchistoryId(TD1.table, TD1.keyColumns, TD1.columns);

        try {
            DataExtractorContext context = (DataExtractorContext) find(CONTEXT_NAME);

            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            long batchId = 998850;
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);

            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, TD1.table, new Date(), history, TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);

            data = new Data(TD2.dataId, TD2.key, TD2.rowData, DataEventType.UPDATE, TD2.table, new Date(), history, TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);
            dataExtractor.commit(batch, writer);

            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(parameterService.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.update(TD2.rowData, TD2.key);
            em.batchEnd(batchId);

            writer.flush();
            Assert.assertEquals(stringWriter.toString(), em.toString());
        } catch (IOException e) {
            Assert.fail("BasicTeset failed");
        }

        reset();
    }

    @Test
    public void notherTest() {
        try {
            DataExtractorContext context = (DataExtractorContext) find(CONTEXT_NAME);

            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            long batchId = 998860;
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);

            TriggerHistory history = makeTableSynchistoryId(TD1.table, TD1.keyColumns, TD1.columns);
            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, TD1.table, new Date(), history, TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);

            history = makeTableSynchistoryId(TD3.table, TD3.keyColumns, TD3.columns);
            data = new Data(TD3.dataId, TD3.key, TD3.rowData, DataEventType.UPDATE, TD3.table, new Date(), history, TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);
            data = new Data(TD3.dataId, TD3.key, TD3.rowData, DataEventType.DELETE, TD3.table, new Date(), history, TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);
            dataExtractor.commit(batch, writer);

            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(parameterService.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.table(TD3.table, TD3.keyColumns, TD3.columns);
            em.update(TD3.rowData, TD3.key);
            em.delete(TD3.key);
            em.batchEnd(batchId);

            writer.flush();
            Assert.assertEquals(stringWriter.toString(), em.toString());
        } catch (IOException e) {
            Assert.fail("BasicTest failed");
        }

        reset();
    }

    @Test
    public void changingTables() {
        TriggerHistory history = makeTableSynchistoryId(TD1.table, TD1.keyColumns, TD1.columns);
        TriggerHistory history2 = makeTableSynchistoryId(TD4.table, TD4.keyColumns, TD4.columns);

        try {
            DataExtractorContext context = (DataExtractorContext)find(CONTEXT_NAME);

            StringWriter stringWriter = new StringWriter();
            BufferedWriter writer = new BufferedWriter(stringWriter);
            dataExtractor.init(writer, context);

            long batchId = 998800;
            OutgoingBatch batch = new OutgoingBatch();
            batch.setBatchId(batchId);
            dataExtractor.begin(batch, writer);

            Data data = new Data(TD1.dataId, TD1.key, TD1.rowData, DataEventType.INSERT, TD1.table, new Date(), history, TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);

            data = new Data(TD4.dataId, TD4.key, TD4.rowData, DataEventType.UPDATE, TD4.table, new Date(), history2,TestConstants.TEST_CHANNEL_ID, null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);

            data = new Data(TD2.dataId, TD2.key, TD2.rowData, DataEventType.UPDATE, TD2.table, new Date(), history,TestConstants.TEST_CHANNEL_ID,  null, null);
            dataExtractor.write(writer, data, Constants.UNKNOWN_ROUTER_ID, context);
            dataExtractor.commit(batch, writer);

            ExpectMaster5000 em = new ExpectMaster5000();
            em.location(parameterService.getExternalId());
            em.batchBegin(batchId);
            em.table(TD1.table, TD1.keyColumns, TD1.columns);
            em.insert(TD1.rowData);
            em.table(TD4.table, TD4.keyColumns, TD4.columns);
            em.update(TD4.rowData, TD4.key);
            em.table(TD1.table);
            em.update(TD2.rowData, TD2.key);
            em.batchEnd(batchId);

            writer.flush();
            Assert.assertEquals(em.toString(), stringWriter.toString());
        } catch (IOException e) {
            Assert.fail("BasicTeset failed");
        }

        reset();
    }

    protected void reset() {
        this.getJdbcTemplate().execute(new ConnectionCallback<Object>() {
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException {
                Statement s = connection.createStatement();
                s.executeUpdate(String.format("delete from sym_trigger_hist where source_table_name in ('%s','%s','%s','%s')", TD1.table, TD2.table, TD3.table, TD4.table));
                return null;
            }
        });
    }

    private TriggerHistory makeTableSynchistoryId(String tableName, final String pk, final String col) {
        String sql = "insert into sym_trigger_hist (trigger_hist_id, source_table_name, source_schema_name, trigger_id, column_names, pk_column_names,name_for_update_trigger,name_for_delete_trigger, name_for_insert_trigger,table_hash,trigger_row_hash,last_trigger_build_reason,create_time) " +
        		" values (null, '"
                + tableName + "','symmetric','1','" + col + "' , '" + pk + "','a','b','c',1,1,'T',current_timestamp)";
        sql = AppUtils.replaceTokens(sql, dbDialect.getSqlScriptReplacementTokens(), false);
        long key = dbDialect.insertWithGeneratedKey(sql, SequenceIdentifier.TRIGGER_HIST);
        TriggerHistory history = new TriggerHistory(tableName, pk, col);
        history.setTriggerHistoryId((int) key);
        return history;
    }

    class ExpectMaster5000 {
        StringWriter base;

        BufferedWriter writer;

        ExpectMaster5000() {
            base = new StringWriter();
            writer = new BufferedWriter(base);
        }

        void location(String location) throws IOException {
            writeCSV(CsvConstants.NODEID);
            writer.write(location);
            writer.newLine();
            writeCSV(CsvConstants.BINARY);
            writer.write(getDbDialect().getBinaryEncoding().name());
            writer.newLine();
        }

        void batchBegin(long batchId) throws IOException {
            writeCSV(CsvConstants.CHANNEL);
            writer.write("null");
            writer.newLine();            
            writeCSV(CsvConstants.BATCH);
            writer.write(new Long(batchId).toString());
            writer.newLine();
        }

        void batchEnd(long batchId) throws IOException {
            writeCSV(CsvConstants.COMMIT);
            writer.write(Long.toString(batchId));
            writer.newLine();
        }

        void table(String tableName, String pk, String cols) throws IOException {
            writeCSV(CsvConstants.SCHEMA);
            writer.newLine();
            writeCSV(CsvConstants.CATALOG);
            writer.newLine();
            writeCSV(CsvConstants.TABLE);
            writer.write(tableName);
            writer.newLine();
            writeCSV(CsvConstants.KEYS);
            writer.write(pk);
            writer.newLine();
            writeCSV(CsvConstants.COLUMNS);
            writer.write(cols);
            writer.newLine();
        }

        void insert(String data) throws IOException {
            writeCSV(CsvConstants.INSERT);
            writer.write(data);
            writer.newLine();
        }

        void update(String rowData, String pk) throws IOException {
            writeCSV(CsvConstants.UPDATE);
            writeCSV(rowData);
            writer.write(pk);
            writer.newLine();
        }

        void delete(String pk) throws IOException {
            writeCSV(CsvConstants.DELETE);
            writer.write(pk);
            writer.newLine();
        }

        void table(String t) throws IOException {
            writeCSV(CsvConstants.SCHEMA);
            writer.newLine();
            writeCSV(CsvConstants.CATALOG);
            writer.newLine();            
            writeCSV(CsvConstants.TABLE);
            writer.write(t);
            writer.newLine();
        }

        private void writeCSV(String constant) throws IOException {
            writer.write(constant);
            writer.write(", ");
        }

        @Override
        public String toString() {
            try {
                writer.flush();
                return base.toString();
            } catch (IOException e) {
                Assert.fail();
            }
            return null;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof String) {
                try {
                    String s = (String) other;
                    writer.flush();
                    String out = base.toString();
                    return out.equals(s);
                } catch (IOException e) {
                    Assert.fail();
                }
            }

            return false;
        }
    }

    class TestData {
        String table;

        String rowData;

        String key;

        long dataId;

        String keyColumns;

        String columns;

        TestData(long dataId, String table, String rowData, String key, String keyColumns, String columns) {
            this.dataId = dataId;
            this.table = table;
            this.rowData = rowData;
            this.key = key;
            this.keyColumns = keyColumns;
            this.columns = columns;
        }
    }

}