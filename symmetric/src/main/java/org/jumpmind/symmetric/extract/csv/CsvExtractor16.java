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

public class CsvExtractor16 extends CsvExtractor14 {

    @Override
    public void write(BufferedWriter writer, Data data, DataExtractorContext context) throws IOException {
        IStreamDataCommand cmd = dictionary.get(data.getEventType().getCode());
        if (cmd.isTriggerHistoryRequired()) {
            preprocessTable(data, writer, context);
        }
        cmd.execute(writer, data, context);
    }

    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * 
     * @param tableName
     * @param out
     */
    @Override
    public void preprocessTable(Data data, BufferedWriter out, DataExtractorContext context) throws IOException {
        if (data.getTriggerHistory() == null) {
            throw new RuntimeException("Missing trigger_hist for table " + data.getTableName()
                    + ": try running syncTriggers() or restarting SymmetricDS");
        } else if (!data.getTriggerHistory().getSourceTableName().toLowerCase().equals(data.getTableName().toLowerCase())) {
            throw new RuntimeException(String.format("The table name captured in the data table (%1$s) does not match the table name recorded in the trigger_hist table (%2$s).  Please drop the symmetric triggers on %1$s and restart the server",  data.getTableName(), data.getTriggerHistory().getSourceTableName() ));
        }
        String triggerHistId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
        if (!context.getHistoryRecordsWritten().contains(triggerHistId)) {
            Util.write(out, CsvConstants.TABLE, ", ", data.getTableName());
            out.newLine();
            Util.write(out, CsvConstants.KEYS, ", ", data.getTriggerHistory().getPkColumnNames());
            out.newLine();
            Util.write(out, CsvConstants.COLUMNS, ", ", data.getTriggerHistory().getColumnNames());
            out.newLine();
            context.getHistoryRecordsWritten().add(triggerHistId);
        } else if (!context.isLastTable(data.getTableName())) {
            Util.write(out, CsvConstants.TABLE, ", ", data.getTableName());
            out.newLine();
        }
        if (data.getEventType() == DataEventType.UPDATE && data.getOldData() != null) {
            Util.write(out, CsvConstants.OLD, ", ", data.getOldData());
            out.newLine();
        }
        context.setLastTableName(data.getTableName());
    }

}
