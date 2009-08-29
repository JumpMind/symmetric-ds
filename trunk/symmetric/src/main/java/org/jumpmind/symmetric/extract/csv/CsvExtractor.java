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

import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.CsvUtils;

public class CsvExtractor extends CsvExtractor16 {

    protected ITriggerRouterService triggerRouterService;
    
    public void init(BufferedWriter writer, DataExtractorContext context) throws IOException {
        super.init(writer, context);
        CsvUtils.write(writer, CsvConstants.BINARY, CsvUtils.DELIMITER, dbDialect.getBinaryEncoding().name());
        writer.newLine();
    }

    @Override
    public void begin(OutgoingBatch batch, BufferedWriter writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.CHANNEL, CsvUtils.DELIMITER, batch.getChannelId());
        writer.newLine();
        CsvUtils.write(writer, CsvConstants.BATCH, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        writer.newLine();
    }
    
    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * @param out
     * @param tableName
     */
    @Override
    public void preprocessTable(Data data, String routerId, BufferedWriter out, DataExtractorContext context) throws IOException {
        if (data.getTriggerHistory() == null) {
            throw new RuntimeException("Missing trigger_hist for table " + data.getTableName()
                    + ": try running syncTriggers() or restarting SymmetricDS");
        } else if (!data.getTriggerHistory().getSourceTableName().toLowerCase().equals(data.getTableName().toLowerCase())) {
            throw new RuntimeException(String.format("The table name captured in the data table (%1$s) does not match the table name recorded in the trigger_hist table (%2$s).  Please drop the symmetric triggers on %1$s and restart the server",  data.getTableName(), data.getTriggerHistory().getSourceTableName() ));
        }
        String triggerHistoryId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
        if (!context.getHistoryRecordsWritten().contains(triggerHistoryId)) {
            writeTable(data, routerId, out);
            CsvUtils.write(out, CsvConstants.KEYS, ", ", data.getTriggerHistory().getPkColumnNames());
            out.newLine();
            CsvUtils.write(out, CsvConstants.COLUMNS, ", ", data.getTriggerHistory().getColumnNames());
            out.newLine();
            context.getHistoryRecordsWritten().add(triggerHistoryId);
        } else if (!context.isLastTable(data.getTableName())) {
            writeTable(data, routerId, out);
        }

        if (data.getEventType() == DataEventType.UPDATE && data.getOldData() != null) {
            CsvUtils.write(out, CsvConstants.OLD, ", ", data.getOldData());
            out.newLine();
        }
        context.setLastTableName(data.getTableName());
    }
    
    protected void writeTable(Data data, String routerId, BufferedWriter out) throws IOException {
        // TODO Add property and write the source schema and the source catalog if set
        Router router = triggerRouterService.getActiveRouterById(parameterService.getNodeGroupId(), routerId, false);
        String schemaName = (router == null || router.getTargetSchemaName() == null) ? "" : router
                .getTargetSchemaName();
        CsvUtils.write(out, CsvConstants.SCHEMA, ", ", schemaName);
        out.newLine();
        String catalogName = (router == null || router.getTargetSchemaName() == null) ? "" : router
                .getTargetCatalogName();
        CsvUtils.write(out, CsvConstants.CATALOG, ", ", catalogName);
        out.newLine();
        String tableName = (router == null || router.getTargetTableName() == null) ? data.getTableName() : router
                .getTargetTableName();
        CsvUtils.write(out, CsvConstants.TABLE, ", ", tableName);
        out.newLine();
    }

    public void setTriggerRouterService(ITriggerRouterService triggerRouterService) {
        this.triggerRouterService = triggerRouterService;
    }
    
}
