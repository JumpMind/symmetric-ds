/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * Copyright (C) Andrew Wilcox <andrewbwilcox@users.sourceforge.net>
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

package org.jumpmind.symmetric.extract.csv;

import java.io.BufferedWriter;
import java.io.IOException;
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

public class CsvExtractor14 implements IDataExtractor {

	private Map<String, String> legacyTableMapping = new HashMap<String, String>();
	
    protected Map<String, IStreamDataCommand> dictionary = null;

    protected IParameterService parameterService;

    protected IDbDialect dbDialect;

    protected INodeService nodeService;

    public CsvExtractor14() {
    	legacyTableMapping.put("sym_trigger", "sym_trigger_old");
    }

    public void init(BufferedWriter writer, DataExtractorContext context) throws IOException {
        Node nodeIdentity = nodeService.findIdentity();
        String nodeId = (nodeIdentity == null) ? parameterService.getString(ParameterConstants.EXTERNAL_ID)
                : nodeIdentity.getNodeId();
        CsvUtils.write(writer, CsvConstants.NODEID, CsvUtils.DELIMITER, nodeId);
        writer.newLine();
    }

    public void begin(OutgoingBatch batch, BufferedWriter writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.BATCH, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        writer.newLine();
        CsvUtils.write(writer, CsvConstants.BINARY, CsvUtils.DELIMITER, dbDialect.getBinaryEncoding().name());
        writer.newLine();
    }

    public void commit(OutgoingBatch batch, BufferedWriter writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.COMMIT, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        writer.newLine();
    }

    public void write(BufferedWriter writer, Data data, String routerId, DataExtractorContext context) throws IOException {
        preprocessTable(data, routerId, writer, context);
        dictionary.get(data.getEventType().getCode()).execute(writer, data, routerId, context);
    }

    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * @param out
     * @param tableName
     */
    public void preprocessTable(Data data, String routerId, BufferedWriter out, DataExtractorContext context) throws IOException {
        if (data.getTriggerHistory() != null) {
            String historyId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
            if (!context.getHistoryRecordsWritten().contains(historyId)) {
                CsvUtils.write(out, CsvConstants.TABLE, ", ", data.getTableName());
                out.newLine();
                CsvUtils.write(out, CsvConstants.KEYS, ", ", data.getTriggerHistory().getPkColumnNames());
                out.newLine();
                CsvUtils.write(out, CsvConstants.COLUMNS, ", ", data.getTriggerHistory().getColumnNames());
                out.newLine();
                context.getHistoryRecordsWritten().add(historyId);
            } else if (!context.isLastTable(data.getTableName())) {
                CsvUtils.write(out, CsvConstants.TABLE, ", ", data.getTableName());
                out.newLine();
            }

            context.setLastTableName(data.getTableName());
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
