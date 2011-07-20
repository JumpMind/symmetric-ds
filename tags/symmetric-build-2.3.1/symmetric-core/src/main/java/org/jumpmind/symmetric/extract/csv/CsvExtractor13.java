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


package org.jumpmind.symmetric.extract.csv;

import java.io.Writer;
import java.io.IOException;
import java.util.Map;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.CsvUtils;

/**
 * 
 *
 * 
 *
 * 
 */
public class CsvExtractor13 implements IDataExtractor {

    private Map<String, IStreamDataCommand> dictionary = null;

    private IParameterService parameterService;

    private IDbDialect dbDialect;

    private String tablePrefix;

    public void init(Writer writer, DataExtractorContext context) throws IOException {
        CsvUtils.write(writer, CsvConstants.NODEID, CsvUtils.DELIMITER, parameterService
                .getString(ParameterConstants.EXTERNAL_ID));
        CsvUtils.writeLineFeed(writer);
    }

    public void begin(OutgoingBatch batch, Writer writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.BATCH, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        CsvUtils.writeLineFeed(writer);
        CsvUtils.write(writer, CsvConstants.BINARY, CsvUtils.DELIMITER, dbDialect.getBinaryEncoding()
                .name());
        CsvUtils.writeLineFeed(writer);
    }

    public void commit(OutgoingBatch batch, Writer writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.COMMIT, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        CsvUtils.writeLineFeed(writer);
    }

    public void write(Writer writer, Data data, String routerId, DataExtractorContext context) throws IOException {
        preprocessTable(data, routerId, writer, context);
        dictionary.get(data.getEventType().getCode()).execute(writer, data, routerId, context);
    }

    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * @param out
     * @param tableName
     */
    public void preprocessTable(Data data, String routerId, Writer out, DataExtractorContext context) throws IOException {

        if (data.getTriggerHistory() == null) {
            throw new RuntimeException("Missing trigger_hist for table " + data.getTableName()
                    + ": try running syncTriggers() or restarting SymmetricDS");
        }
        String historyId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
        if (!context.getHistoryRecordsWritten().contains(historyId)) {
            CsvUtils.write(out, "table, ", data.getTableName());
            CsvUtils.writeLineFeed(out);
            CsvUtils.write(out, "keys, ", data.getTriggerHistory().getPkColumnNames());
            CsvUtils.writeLineFeed(out);
            String columns = data.getTriggerHistory().getColumnNames();
            if (data.getTableName().equalsIgnoreCase(tablePrefix + "_node_security")) {
                // In 1.4 the column named changed to "node_password", but old
                // clients need "password"
                columns = columns.replaceFirst(",node_password,", ",password,");
                columns = columns.replaceFirst(",NODE_PASSWORD,", ",PASSWORD,");
            }
            CsvUtils.write(out, "columns, ", columns);
            CsvUtils.writeLineFeed(out);
            context.getHistoryRecordsWritten().add(historyId);
        } else if (!context.isLastDataFromSameTriggerAndRouter(historyId, routerId)) {
            CsvUtils.write(out, "table, ", data.getTableName());
            CsvUtils.writeLineFeed(out);
        }

        context.setLastRouterId(routerId);
        context.setLastTriggerHistoryId(historyId);
    }

    public void setDictionary(Map<String, IStreamDataCommand> dictionary) {
        this.dictionary = dictionary;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
    public String getLegacyTableName(String currentTableName) {
        return currentTableName;
    }
}