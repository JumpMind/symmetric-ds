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
 * under the License. 
 */
package org.jumpmind.symmetric.io.data.writer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractProtocolDataWriter implements IDataWriter {
    
    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected DataContext context;

    protected Batch batch;

    protected Table table;

    protected Map<String, Table> processedTables = new HashMap<String, Table>();

    protected String delimiter = ",";

    protected boolean flushNodeId = true;

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected List<IProtocolDataWriterListener> listeners;

    protected String sourceNodeId;
    
    protected boolean noBinaryOldData = false;

    public AbstractProtocolDataWriter(String sourceNodeId,
            List<IProtocolDataWriterListener> listeners) {
        this.listeners = listeners;
        this.sourceNodeId = sourceNodeId;
    }

    public void open(DataContext context) {
        this.context = context;
    }

    public void close() {
    }

    public void start(Batch batch) {
        this.statistics.put(batch, new Statistics());
        this.batch = batch;

        if (listeners != null) {
            for (IProtocolDataWriterListener listener : listeners) {
                listener.start(context, batch);
            }
        }
        
        if (StringUtils.isBlank(sourceNodeId)) {
            sourceNodeId = batch.getSourceNodeId();
        }

        if (flushNodeId) {
            if (StringUtils.isNotBlank(sourceNodeId)) {
                println(CsvConstants.NODEID, sourceNodeId);
            }
            BinaryEncoding binaryEncoding = batch.getBinaryEncoding();
            if (binaryEncoding != null) {
                println(CsvConstants.BINARY, binaryEncoding.name());
            }
            flushNodeId = false;
        }
        if (StringUtils.isNotBlank(batch.getChannelId())) {
            println(CsvConstants.CHANNEL, batch.getChannelId());
        }
        println(CsvConstants.BATCH, Long.toString(batch.getBatchId()));
    }

    public boolean start(Table table) {
        if (!batch.isIgnored()) {
            this.table = table;
            String catalogName = table.getCatalog();
            println(CsvConstants.CATALOG, StringUtils.isNotBlank(catalogName) ? catalogName : "");
            String schemaName = table.getSchema();
            println(CsvConstants.SCHEMA, StringUtils.isNotBlank(schemaName) ? schemaName : "");
            println(CsvConstants.TABLE, table.getName());
            if (!processedTables.containsKey(table.getName()) || 
                    !processedTables.get(table.getName()).equals(table)) {
                println(CsvConstants.KEYS, table.getPrimaryKeyColumns());
                println(CsvConstants.COLUMNS, table.getColumns());
                this.processedTables.put(table.getName(), table);
            }
            return true;
        } else {
            return false;
        }
    }

    public void write(CsvData data) {
        if (!batch.isIgnored()) {
            
            if (noBinaryOldData != data.isNoBinaryOldData()) {
                noBinaryOldData = data.isNoBinaryOldData();
                println(CsvConstants.NO_BINARY_OLD_DATA, Boolean.toString(noBinaryOldData));
            }
            
            statistics.get(batch).increment(DataWriterStatisticConstants.STATEMENTCOUNT);
            statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
            switch (data.getDataEventType()) {
                case INSERT:
                    println(CsvConstants.INSERT, data.getCsvData(CsvData.ROW_DATA));
                    break;

                case UPDATE:
                    String oldData = data.getCsvData(CsvData.OLD_DATA);
                    if (StringUtils.isNotBlank(oldData)) {
                        println(CsvConstants.OLD, oldData);
                    }
                    println(CsvConstants.UPDATE, data.getCsvData(CsvData.ROW_DATA),
                            data.getCsvData(CsvData.PK_DATA));
                    break;

                case DELETE:
                    oldData = data.getCsvData(CsvData.OLD_DATA);
                    if (StringUtils.isNotBlank(oldData)) {
                        println(CsvConstants.OLD, oldData);
                    }
                    println(CsvConstants.DELETE, data.getCsvData(CsvData.PK_DATA));
                    break;

                case CREATE:
                    println(CsvConstants.CREATE, data.getCsvData(CsvData.ROW_DATA));
                    break;

                case BSH:
                    println(CsvConstants.BSH, data.getCsvData(CsvData.ROW_DATA));
                    break;

                case SQL:
                    println(CsvConstants.SQL, data.getCsvData(CsvData.ROW_DATA));
                    break;
                    
                case RELOAD:
                default:
                    break;                      
            }
        }
    }

    public void end(Table table) {
    }

    final public void end(Batch batch, boolean inError) {
        
        if (batch.isIgnored()) {
            println(CsvConstants.IGNORE);
        }
        
        if (!inError) {
            println(CsvConstants.COMMIT, Long.toString(batch.getBatchId()));
            endBatch(batch);
        }

        if (listeners != null && !inError) {
            for (IProtocolDataWriterListener listener : listeners) {
                notifyEndBatch(batch, listener);
            }
        }
    }

    abstract protected void endBatch(Batch batch);

    abstract protected void notifyEndBatch(Batch batch, IProtocolDataWriterListener listener);

    protected int println(String key, List<Column> columns) {
        return println(key, columns.toArray(new Column[columns.size()]));
    }

    protected int println(String key, Column[] columns) {
        StringBuilder buffer = new StringBuilder(key);
        for (int i = 0; i < columns.length; i++) {
            buffer.append(delimiter);
            buffer.append(columns[i].getName());
        }
        println(buffer.toString());
        return buffer.length();
    }

    abstract protected void print(Batch batch, String data);

    protected long println(String... data) {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            if (i != 0) {
                buffer.append(delimiter);
            }
            buffer.append(data[i]);
        }
        buffer.append("\n");
        print(batch, buffer.toString());
        long byteCount = buffer.length();
        statistics.get(batch).increment(DataWriterStatisticConstants.BYTECOUNT, byteCount);
        return byteCount;
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

}
