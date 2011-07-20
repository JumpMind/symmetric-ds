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

import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.util.CsvUtils;

/**
 * @see IDataExtractor 
 */
public class CsvExtractor16 extends CsvExtractor14 {

	@Override
    public void write(Writer writer, Data data, String routerId, DataExtractorContext context)
            throws IOException {
        IStreamDataCommand cmd = dictionary.get(data.getEventType().getCode());
        if (cmd == null) {
            throw new SymmetricException("DataExtractorCouldNotFindStreamCommand", data.getEventType().getCode());
        } else if (cmd.isTriggerHistoryRequired()) {
            preprocessTable(data, routerId, writer, context);
        }
        cmd.execute(writer, data, routerId, context);
    }

    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * 
     * @param out
     * @param tableName
     */
    @Override
    public void preprocessTable(Data data, String routerId, Writer out, DataExtractorContext context)
            throws IOException {
        if (data.getTriggerHistory() == null) {
            throw new RuntimeException("Missing trigger_hist for table " + data.getTableName()
                    + ": try running syncTriggers() or restarting SymmetricDS");
        } else if (!data.getTriggerHistory().getSourceTableName().toLowerCase().equals(
                data.getTableName().toLowerCase())) {
            throw new RuntimeException(
                    String
                            .format(
                                    "The table name captured in the data table (%1$s) does not match the table name recorded in the trigger_hist table (%2$s).  Please drop the symmetric triggers on %1$s and restart the server",
                                    data.getTableName(), data.getTriggerHistory().getSourceTableName()));
        }
        String triggerHistId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
        if (!context.getHistoryRecordsWritten().contains(triggerHistId)) {
            
            context.incrementByteCount(CsvUtils.write(out, CsvConstants.TABLE, ", ", data.getTableName()));
            CsvUtils.writeLineFeed(out);
            context.incrementByteCount(CsvUtils.write(out, CsvConstants.KEYS, ", ", data.getTriggerHistory().getPkColumnNames()));
            CsvUtils.writeLineFeed(out);
            context.incrementByteCount(CsvUtils.write(out, CsvConstants.COLUMNS, ", ", data.getTriggerHistory().getColumnNames()));
            CsvUtils.writeLineFeed(out);
            context.getHistoryRecordsWritten().add(triggerHistId);
        } else if (!context.isLastDataFromSameTriggerAndRouter(triggerHistId, routerId)) {
            context.incrementByteCount(CsvUtils.write(out, CsvConstants.TABLE, ", ", data.getTableName()));
            CsvUtils.writeLineFeed(out);
        }

        if (data.getEventType() == DataEventType.UPDATE && data.getOldData() != null && parameterService.is(ParameterConstants.DATA_EXTRACTOR_OLD_DATA_ENABLED)) {
            CsvUtils.write(out, CsvConstants.OLD, ", ", data.getOldData());
            CsvUtils.writeLineFeed(out);
        }
        context.setLastRouterId(routerId);
        context.setLastTriggerHistoryId(triggerHistId);
    }

}