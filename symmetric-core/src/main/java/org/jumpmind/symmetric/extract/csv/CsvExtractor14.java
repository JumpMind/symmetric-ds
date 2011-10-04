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

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.CsvUtils;

/**
 * @see IDataExtractor 
 */
public class CsvExtractor14 implements IDataExtractor {

	private Map<String, String> legacyTableMapping = new HashMap<String, String>();
	
    protected Map<String, IStreamDataCommand> dictionary = null;

    protected IParameterService parameterService;

    protected IDbDialect dbDialect;

    protected INodeService nodeService;

    public CsvExtractor14() {
    	legacyTableMapping.put("sym_trigger", "sym_trigger_old");
    }

    public void init(Writer writer, DataExtractorContext context) throws IOException {
        Node nodeIdentity = nodeService.findIdentity();
        String nodeId = (nodeIdentity == null) ? parameterService.getString(ParameterConstants.EXTERNAL_ID)
                : nodeIdentity.getNodeId();
        context.incrementByteCount(CsvUtils.write(writer, CsvConstants.NODEID, CsvUtils.DELIMITER, nodeId));
        CsvUtils.writeLineFeed(writer);
    }

    public void begin(OutgoingBatch batch, Writer writer) throws IOException {
        batch.incrementByteCount(CsvUtils.write(writer, CsvConstants.BATCH, CsvUtils.DELIMITER, Long.toString(batch.getBatchId())));
        CsvUtils.writeLineFeed(writer);
        batch.incrementByteCount(CsvUtils.write(writer, CsvConstants.BINARY, CsvUtils.DELIMITER, dbDialect.getBinaryEncoding().name()));
        CsvUtils.writeLineFeed(writer);
    }

    public void commit(OutgoingBatch batch, Writer writer) throws IOException {
        batch.incrementByteCount(CsvUtils.write(writer, CsvConstants.COMMIT, CsvUtils.DELIMITER, Long.toString(batch.getBatchId())));
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
        if (data.getTriggerHistory() != null) {
            String historyId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
            if (!context.getHistoryRecordsWritten().contains(historyId)) {
                CsvUtils.write(out, CsvConstants.TABLE, ", ", data.getTableName());
                CsvUtils.writeLineFeed(out);
                CsvUtils.write(out, CsvConstants.KEYS, ", ", data.getTriggerHistory().getPkColumnNames());
                CsvUtils.writeLineFeed(out);
                CsvUtils.write(out, CsvConstants.COLUMNS, ", ", data.getTriggerHistory().getColumnNames());
                CsvUtils.writeLineFeed(out);
                context.addHistoryRecordWritten(data.getTableName(), historyId);
            } else if (!context.isLastDataFromSameTriggerAndRouter(historyId, routerId)) {
                CsvUtils.write(out, CsvConstants.TABLE, ", ", data.getTableName());
                CsvUtils.writeLineFeed(out);
            }

            context.setLastRouterId(routerId);
            context.setLastTriggerHistoryId(historyId);
        }
    }

    public void setDictionary(Map<String, IStreamDataCommand> dictionary) {
        this.dictionary = dictionary;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public String getLegacyTableName(String currentTableName) {
        String result = currentTableName;

        if (legacyTableMapping.get(currentTableName.toLowerCase()) != null) {
            result = legacyTableMapping.get(currentTableName.toLowerCase());
        }
        return result;
    }

}