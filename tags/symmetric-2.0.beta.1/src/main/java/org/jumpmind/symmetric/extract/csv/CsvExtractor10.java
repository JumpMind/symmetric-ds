/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 * Copyright (C) Andrew Wilcox <andrewbwilcox@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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
import java.util.Map;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.CsvUtils;

public class CsvExtractor10 implements IDataExtractor {

    private Map<String, IStreamDataCommand> dictionary = null;

    private IParameterService parameterService;

    private String tablePrefix;

    public void init(BufferedWriter writer, DataExtractorContext context) throws IOException {
        CsvUtils.write(writer, CsvConstants.NODEID, CsvUtils.DELIMITER, parameterService
                .getString(ParameterConstants.EXTERNAL_ID));
        writer.newLine();
    }

    public void begin(OutgoingBatch batch, BufferedWriter writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.BATCH, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        writer.newLine();
    }

    public void commit(OutgoingBatch batch, BufferedWriter writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.COMMIT, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        writer.newLine();
    }

    public void write(BufferedWriter writer, Data data, String routerId, DataExtractorContext context) throws IOException {
        preprocessTable(data, routerId, writer, context);
        dictionary.get(data.getEventType().getCode()).execute(writer, data, context);
    }

    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * @param out
     * @param tableName
     */
    public void preprocessTable(Data data, String routerId, BufferedWriter out, DataExtractorContext context) throws IOException {

        String historyId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
        if (!context.getHistoryRecordsWritten().contains(historyId)) {
            CsvUtils.write(out, "table, ", data.getTableName());
            out.newLine();
            CsvUtils.write(out, "keys, ", data.getTriggerHistory().getPkColumnNames());
            out.newLine();
            String columns = data.getTriggerHistory().getColumnNames();
            if (data.getTableName().equalsIgnoreCase(tablePrefix + "_node_security")) {
                // In 1.4 the column named changed to "node_password", but old
                // clients need "password"
                columns = columns.replaceFirst(",node_password,", ",password,");
                columns = columns.replaceFirst(",NODE_PASSWORD,", ",PASSWORD,");
            }
            CsvUtils.write(out, "columns, ", columns);
            out.newLine();
            context.getHistoryRecordsWritten().add(historyId);
        } else if (!context.isLastTable(data.getTableName())) {
            CsvUtils.write(out, "table, ", data.getTableName());
            out.newLine();
        }

        context.setLastTableName(data.getTableName());
    }

    public void setDictionary(Map<String, IStreamDataCommand> dictionary) {
        this.dictionary = dictionary;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

}
