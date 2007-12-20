/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.OutgoingBatch;

public class CsvExtractor implements IDataExtractor {

    private Map<String, IStreamDataCommand> dictionary = null;

    private IRuntimeConfig runtimeConfiguration;

    public void init(BufferedWriter writer, DataExtractorContext context)
            throws IOException {
        Util.write(writer, CsvConstants.NODEID, AbstractStreamDataCommand.DELIMITER, runtimeConfiguration.getExternalId());
        writer.newLine();
    }

    public void begin(OutgoingBatch batch, BufferedWriter writer)
            throws IOException {
        Util.write(writer, CsvConstants.BATCH, AbstractStreamDataCommand.DELIMITER, batch.getBatchId());
        writer.newLine();
    }

    public void commit(OutgoingBatch batch, BufferedWriter writer)
            throws IOException {
        Util.write(writer, CsvConstants.COMMIT, AbstractStreamDataCommand.DELIMITER, batch.getBatchId());
        writer.newLine();
    }

    public void write(BufferedWriter writer, Data data,
            DataExtractorContext context) throws IOException {
        preprocessTable(data, writer, context);
        dictionary.get(data.getEventType().getCode()).execute(writer, data, context);
    }

    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * 
     * @param tableName
     * @param out
     */
    public void preprocessTable(Data data, BufferedWriter out,
            DataExtractorContext context) throws IOException {

        String auditKey = Integer.toString(data.getAudit().getTriggerHistoryId()).intern();
        if (!context.getAuditRecordsWritten().contains(auditKey)) {
            Util.write(out, "table, ", data.getTableName());
            out.newLine();
            Util.write(out, "keys, ", data.getAudit().getPkColumnNames());
            out.newLine();
            Util.write(out, "columns, ", data.getAudit().getColumnNames());
            out.newLine();
            context.getAuditRecordsWritten().add(auditKey);
        } else if (!context.isLastTable(data.getTableName())) {
            Util.write(out, "table, ", data.getTableName());
            out.newLine();
        }
        
        context.setLastTableName(data.getTableName());
    }

    public void setRuntimeConfiguration(IRuntimeConfig runtimeConfiguration) {
        this.runtimeConfiguration = runtimeConfiguration;
    }

    public void setDictionary(Map<String, IStreamDataCommand> dictionary) {
        this.dictionary = dictionary;
    }

}
