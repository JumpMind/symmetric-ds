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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IDataExtractor;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.CsvUtils;

/**
 * @see IDataExtractor 
 */
public class CsvExtractor extends CsvExtractor16 {

    protected ITriggerRouterService triggerRouterService;
    
    public void init(Writer writer, DataExtractorContext context) throws IOException {
        super.init(writer, context);
        CsvUtils.write(writer, CsvConstants.BINARY, CsvUtils.DELIMITER, dbDialect.getBinaryEncoding().name());
        CsvUtils.writeLineFeed(writer);
    }

    @Override
    public void begin(OutgoingBatch batch, Writer writer) throws IOException {
        CsvUtils.write(writer, CsvConstants.CHANNEL, CsvUtils.DELIMITER, batch.getChannelId());
        CsvUtils.writeLineFeed(writer);
        CsvUtils.write(writer, CsvConstants.BATCH, CsvUtils.DELIMITER, Long.toString(batch.getBatchId()));
        CsvUtils.writeLineFeed(writer);
    }
    
    /**
     * Writes the table metadata out to a stream only if it hasn't already been
     * written out before
     * @param out
     * @param tableName
     */
    @Override
    public void preprocessTable(Data data, String routerId, Writer out, DataExtractorContext context) throws IOException {
        if (data.getTriggerHistory() == null) {
            throw new RuntimeException("Missing trigger_hist for table " + data.getTableName()
                    + ": try running syncTriggers() or restarting SymmetricDS");
        } else if (!data.getTriggerHistory().getSourceTableName().toLowerCase().equals(data.getTableName().toLowerCase())) {
            throw new RuntimeException(String.format("The table name captured in the data table (%1$s) does not match the table name recorded in the trigger_hist table (%2$s).  Please drop the symmetric triggers on %1$s and restart the server",  data.getTableName(), data.getTriggerHistory().getSourceTableName() ));
        }
        
        String triggerHistoryId = Integer.toString(data.getTriggerHistory().getTriggerHistoryId()).intern();
        if (!context.getHistoryRecordsWritten().contains(triggerHistoryId)) {
            writeTable(data, routerId, out, context);
            context.incrementByteCount(CsvUtils.write(out, CsvConstants.KEYS, ", ", data.getTriggerHistory().getPkColumnNames()));
            CsvUtils.writeLineFeed(out);
            context.incrementByteCount(CsvUtils.write(out, CsvConstants.COLUMNS, ", ", data.getTriggerHistory().getColumnNames()));
            CsvUtils.writeLineFeed(out);
            context.addHistoryRecordWritten(data.getTableName(), triggerHistoryId);
        } else if (!context.isLastDataFromSameTriggerAndRouter(triggerHistoryId, routerId)) {
            writeTable(data, routerId, out, context);
        }

        if (data.getEventType() == DataEventType.UPDATE && data.getOldData() != null && parameterService.is(ParameterConstants.DATA_EXTRACTOR_OLD_DATA_ENABLED)) {
            context.incrementByteCount(CsvUtils.write(out, CsvConstants.OLD, ", ", data.getOldData()));
            CsvUtils.writeLineFeed(out);
        }
        context.setLastRouterId(routerId);
        context.setLastTriggerHistoryId(triggerHistoryId);
    }
    
    protected void writeTable(Data data, String routerId, Writer out, DataExtractorContext context) throws IOException {
        // TODO Add property and write the source schema and the source catalog if set
        Router router = triggerRouterService.getActiveRouterByIdForCurrentNode(routerId, false);
        String schemaName = router == null ? "" : getTargetName(router
                .getTargetSchemaName());
        context.incrementByteCount(CsvUtils.write(out, CsvConstants.SCHEMA, ", ", schemaName));
        CsvUtils.writeLineFeed(out);
        String catalogName = router == null ? "" : getTargetName(router
                .getTargetCatalogName());
        context.incrementByteCount(CsvUtils.write(out, CsvConstants.CATALOG, ", ", catalogName));
        CsvUtils.writeLineFeed(out);
        String tableName = (router == null || router.getTargetTableName() == null) ? data.getTableName() : 
            getTargetName(router.getTargetTableName());
        context.incrementByteCount(CsvUtils.write(out, CsvConstants.TABLE, ", ", tableName));
        CsvUtils.writeLineFeed(out);
    }
    
    protected String getTargetName(String name) {
        String catalogName = name == null ? "" : name;
        if (StringUtils.isNotBlank(catalogName)) {
            catalogName = AppUtils.replaceTokens(catalogName, parameterService.getReplacementValues(), true);
        }
        return catalogName;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerRouterService) {
        this.triggerRouterService = triggerRouterService;
    }
    
    public String getLegacyTableName(String currentTableName){
        return currentTableName;
    }
    
}