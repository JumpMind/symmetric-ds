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
package org.jumpmind.symmetric.io.data.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.Statistics;

public class ProtocolDataReader extends AbstractDataReader implements IDataReader {

    public static final String CTX_LINE_NUMBER = ProtocolDataReader.class.getSimpleName()
            + ".lineNumber";

    protected IStagedResource stagedResource;
    protected Reader reader;
    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();
    protected CsvReader csvReader;
    protected DataContext context;
    protected Map<String, Table> tables = new HashMap<String, Table>();
    protected Object next;
    protected Batch batch;
    protected Table table;
    protected String channelId;
    protected String sourceNodeId;
    protected String targetNodeId;
    protected BinaryEncoding binaryEncoding;
    protected boolean noBinaryOldData = false;
    protected BatchType batchType;
    protected int lineNumber = 0;

    public ProtocolDataReader(BatchType batchType, String targetNodeId, StringBuilder input) {
        this(batchType, targetNodeId, new BufferedReader(new StringReader(input.toString())));
    }

    public ProtocolDataReader(BatchType batchType, String targetNodeId, InputStream is) {
        this(batchType, targetNodeId, toReader(is));
    }

    public ProtocolDataReader(BatchType batchType, String targetNodeId,
            IStagedResource stagedResource) {
        this.stagedResource = stagedResource;
        this.targetNodeId = targetNodeId;
        this.batchType = batchType;
    }

    public ProtocolDataReader(BatchType batchType, String targetNodeId, String input) {
        this(batchType, targetNodeId, new BufferedReader(new StringReader(input)));
    }

    public ProtocolDataReader(BatchType batchType, String targetNodeId, Reader reader) {
        this.reader = reader;
        this.targetNodeId = targetNodeId;
        this.batchType = batchType;
    }

    public ProtocolDataReader(BatchType batchType, String targetNodeId, File file) {
        try {
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader in = new InputStreamReader(fis, "UTF-8");
            this.targetNodeId = targetNodeId;
            this.batchType = batchType;
            this.reader = new BufferedReader(in);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public IStagedResource getStagedResource() {
        return stagedResource;
    }

    public void open(DataContext context) {
        if (this.stagedResource != null && this.reader == null) {
            this.reader = this.stagedResource.getReader();
        }
        this.lineNumber = 0;
        this.context = context;
        this.csvReader = CsvUtils.getCsvReader(reader);
        this.next = readNext();
    }

    protected Object readNext() {
        try {
            Set<String> keys = null;
            String schemaName = null;
            String catalogName = null;
            String[] parsedOldData = null;
            long bytesRead = 0;
            while (csvReader.readRecord()) {
                lineNumber++;
                context.put(CTX_LINE_NUMBER, lineNumber);
                String[] tokens = csvReader.getValues();
                bytesRead += logDebugAndCountBytes(tokens);
                if (batch != null) {
                    statistics.get(batch)
                            .increment(DataReaderStatistics.READ_BYTE_COUNT, bytesRead);
                    bytesRead = 0;
                }
                if (tokens[0].equals(CsvConstants.INSERT)) {
                    CsvData data = new CsvData();
                    data.setNoBinaryOldData(noBinaryOldData);
                    data.setDataEventType(DataEventType.INSERT);
                    data.putParsedData(CsvData.ROW_DATA,
                            CollectionUtils.copyOfRange(tokens, 1, tokens.length));
                    return data;
                } else if (tokens[0].equals(CsvConstants.OLD)) {
                    parsedOldData = CollectionUtils.copyOfRange(tokens, 1, tokens.length);
                } else if (tokens[0].equals(CsvConstants.UPDATE)) {
                    CsvData data = new CsvData();
                    data.setNoBinaryOldData(noBinaryOldData);
                    data.setDataEventType(DataEventType.UPDATE);
                    // TODO check for invalid range and print results
                    data.putParsedData(CsvData.ROW_DATA,
                            CollectionUtils.copyOfRange(tokens, 1, table.getColumnCount() + 1));
                    data.putParsedData(CsvData.PK_DATA, CollectionUtils.copyOfRange(tokens,
                            table.getColumnCount() + 1, tokens.length));
                    data.putParsedData(CsvData.OLD_DATA, parsedOldData);
                    return data;
                } else if (tokens[0].equals(CsvConstants.DELETE)) {
                    CsvData data = new CsvData();
                    data.setNoBinaryOldData(noBinaryOldData);
                    data.setDataEventType(DataEventType.DELETE);
                    data.putParsedData(CsvData.PK_DATA,
                            CollectionUtils.copyOfRange(tokens, 1, tokens.length));
                    data.putParsedData(CsvData.OLD_DATA, parsedOldData);
                    return data;

                } else if (tokens[0].equals(CsvConstants.BATCH)) {
                    Batch batch = new Batch(batchType, Long.parseLong(tokens[1]), channelId,
                            binaryEncoding, sourceNodeId, targetNodeId, false);
                    statistics.put(batch, new DataReaderStatistics());
                    return batch;
                } else if (tokens[0].equals(CsvConstants.NO_BINARY_OLD_DATA)) {
                    if (tokens.length > 1) {
                        noBinaryOldData = Boolean.parseBoolean(tokens[1]);
                    }
                } else if (tokens[0].equals(CsvConstants.NODEID)) {
                    this.sourceNodeId = tokens[1];
                } else if (tokens[0].equals(CsvConstants.BINARY)) {
                    this.binaryEncoding = BinaryEncoding.valueOf(tokens[1]);
                } else if (tokens[0].equals(CsvConstants.CHANNEL)) {
                    this.channelId = tokens[1];
                } else if (tokens[0].equals(CsvConstants.SCHEMA)) {
                    schemaName = tokens.length == 1 || StringUtils.isBlank(tokens[1]) ? null
                            : tokens[1];
                } else if (tokens[0].equals(CsvConstants.CATALOG)) {
                    catalogName = tokens.length == 1 || StringUtils.isBlank(tokens[1]) ? null
                            : tokens[1];
                } else if (tokens[0].equals(CsvConstants.TABLE)) {
                    String tableName = tokens[1];
                    table = tables.get(Table.getFullyQualifiedTableName(catalogName, schemaName,
                            tableName));
                    if (table != null) {
                        return table;
                    } else {
                        table = new Table(catalogName, schemaName, tableName);
                    }
                } else if (tokens[0].equals(CsvConstants.KEYS)) {
                    if (keys == null) {
                        keys = new HashSet<String>(tokens.length);
                    }
                    for (int i = 1; i < tokens.length; i++) {
                        keys.add(tokens[i]);
                    }
                } else if (tokens[0].equals(CsvConstants.COLUMNS)) {
                    table.removeAllColumns();
                    for (int i = 1; i < tokens.length; i++) {
                        Column column = new Column(tokens[i], keys != null
                                && keys.contains(tokens[i]));
                        table.addColumn(column);
                    }
                    tables.put(table.getFullyQualifiedTableName(), table);
                    return table;
                } else if (tokens[0].equals(CsvConstants.COMMIT)) {
                    if (batch != null) {
                        batch.setComplete(true);
                    }
                    return null;
                } else if (tokens[0].equals(CsvConstants.SQL)) {
                    CsvData data = new CsvData();
                    data.setNoBinaryOldData(noBinaryOldData);
                    data.setDataEventType(DataEventType.SQL);
                    data.putParsedData(CsvData.ROW_DATA, new String[] { tokens[1] });
                    return data;
                } else if (tokens[0].equals(CsvConstants.BSH)) {
                    CsvData data = new CsvData();
                    data.setNoBinaryOldData(noBinaryOldData);
                    data.setDataEventType(DataEventType.BSH);
                    data.putParsedData(CsvData.ROW_DATA, new String[] { tokens[1] });
                    return data;
                } else if (tokens[0].equals(CsvConstants.CREATE)) {
                    CsvData data = new CsvData();
                    data.setNoBinaryOldData(noBinaryOldData);
                    data.setDataEventType(DataEventType.CREATE);
                    data.putParsedData(CsvData.ROW_DATA, new String[] { tokens[1] });
                    return data;
                } else if (tokens[0].equals(CsvConstants.IGNORE)) {
                    if (batch != null) {
                        batch.setIgnored(true);
                    }
                } else {
                    log.info("Unable to handle unknown csv values: " + Arrays.toString(tokens));
                }
            }
        } catch (IOException ex) {
            throw new IoException(ex);
        }

        return null;

    }

    public Batch nextBatch() {
        if (next instanceof Batch) {
            this.batch = (Batch) next;
            next = null;
            return batch;
        } else {
            do {
                next = readNext();
                if (next instanceof Batch) {
                    this.batch = (Batch) next;
                    next = null;
                    return batch;
                }
            } while (next != null);
        }
        return null;
    }

    public Table nextTable() {
        if (next instanceof Table) {
            this.table = (Table) next;
            next = null;
            return table;
        } else {
            do {
                next = readNext();
                if (next instanceof Table) {
                    this.table = (Table) next;
                    next = null;
                    return table;
                }
            } while (next != null && !(next instanceof Batch));
        }
        return null;
    }

    public CsvData nextData() {
        if (next instanceof CsvData) {
            CsvData data = (CsvData) next;
            next = null;
            return data;
        } else {
            do {
                next = readNext();
                if (next instanceof CsvData) {
                    CsvData data = (CsvData) next;
                    next = null;
                    return data;
                }
            } while (next != null && !(next instanceof Batch) && !(next instanceof Table));
        }
        return null;
    }

    public void close() {
        if (csvReader != null) {
            csvReader.close();
        }

        if (stagedResource != null) {
            stagedResource.close();
        }

    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

}
