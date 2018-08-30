/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractDataReader implements IDataReader {
    
    protected static final Logger log = LoggerFactory.getLogger(ExtractDataReader.class);    
    
    public static final String DATA_CONTEXT_CURRENT_CSV_DATA = "csvData"; 

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected IDatabasePlatform platform;

    protected List<IExtractDataReaderSource> sourcesToUse;

    protected IExtractDataReaderSource currentSource;

    protected Batch batch;

    protected Table table;

    protected CsvData data;
    
    protected DataContext dataContext;

    public ExtractDataReader(IDatabasePlatform platform, IExtractDataReaderSource source) {
        this.sourcesToUse = new ArrayList<IExtractDataReaderSource>();
        this.sourcesToUse.add(source);
        this.platform = platform;
    }

    public ExtractDataReader(IDatabasePlatform platform, List<IExtractDataReaderSource> sources) {
        this.sourcesToUse = new ArrayList<IExtractDataReaderSource>(sources);
        this.platform = platform;
    }

    public void open(DataContext context) {
        this.dataContext = context;
    }

    public Batch nextBatch() {
        closeCurrentSource();
        if (this.sourcesToUse.size() > 0) {
            this.currentSource = this.sourcesToUse.remove(0);
            this.batch = this.currentSource.getBatch();
        } else {
            this.batch = null;
        }
        return this.batch;

    }

    public Table nextTable() {
        this.table = null;
        if (this.currentSource != null) {
            if (this.data == null) {
                this.data = this.currentSource.next();
            }
            if (this.data != null) {
                this.table = this.currentSource.getTargetTable();
                if (this.table != null) {
                    this.table.setCatalog(substituteVariables(this.table.getCatalog()));
                    this.table.setSchema(substituteVariables(this.table.getSchema()));
                }
            }
        }
        
        if (this.table == null && this.batch != null) {
            this.batch.setComplete(true);
        }
        return this.table;
    }

    protected String substituteVariables(String sourceString) {
        if (sourceString != null && sourceString.indexOf("$(") != -1) {
            sourceString = FormatUtils.replace("sourceNodeId", (String) dataContext.get("sourceNodeId"), sourceString);
            sourceString = FormatUtils.replace("sourceNodeExternalId", (String) dataContext.get("sourceNodeExternalId"), sourceString);
            sourceString = FormatUtils.replace("sourceNodeGroupId", (String) dataContext.get("sourceNodeGroupId"), sourceString);
            sourceString = FormatUtils.replace("targetNodeId", (String) dataContext.get("targetNodeId"), sourceString);
            sourceString = FormatUtils.replace("targetNodeExternalId", (String) dataContext.get("targetNodeExternalId"), sourceString);
            sourceString = FormatUtils.replace("targetNodeGroupId", (String) dataContext.get("targetNodeGroupId"), sourceString);
        }
        return sourceString;
    }

    public CsvData nextData() {
        if (this.table != null) {
            if (this.data == null) {
                this.data = this.currentSource.next();
            }

            if (data == null) {
                closeCurrentSource();
            } else {
                Table targetTable = this.currentSource.getTargetTable();
                if (targetTable != null && targetTable.equals(this.table)) {
                    data = enhanceWithLobsFromSourceIfNeeded(this.currentSource.getSourceTable(), data);
                } else {
                    // the table has changed
                    return null;
                }
            }
        }

        CsvData dataToReturn = this.data;
        this.data = null;
        this.dataContext.put(DATA_CONTEXT_CURRENT_CSV_DATA, dataToReturn);
        return dataToReturn;
    }

    public void close() {
        closeCurrentSource();
        this.batch = null;
    }

    protected void closeCurrentSource() {
        if (this.currentSource != null) {
            this.currentSource.close();
            this.currentSource = null;
        }

        this.table = null;
        this.data = null;
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

    protected CsvData enhanceWithLobsFromSourceIfNeeded(Table table, CsvData data) {
        if (this.currentSource.requiresLobsSelectedFromSource(data)
                && (data.getDataEventType() == DataEventType.UPDATE || data.getDataEventType() == DataEventType.INSERT)) {
            List<Column> lobColumns = platform.getLobColumns(table);
            if (lobColumns.size() > 0) {
                String[] columnNames = table.getColumnNames();
                String[] rowData = data.getParsedData(CsvData.ROW_DATA);
                Column[] orderedColumns = table.getColumns();
                Object[] objectValues = platform.getObjectValues(batch.getBinaryEncoding(),
                        rowData, orderedColumns);
                Map<String, Object> columnDataMap = CollectionUtils
                        .toMap(columnNames, objectValues);
                Column[] pkColumns = table.getPrimaryKeyColumns();
                ISqlTemplate sqlTemplate = platform.getSqlTemplate();
                Object[] args = new Object[pkColumns.length];
                for (int i = 0; i < pkColumns.length; i++) {
                    args[i] = columnDataMap.get(pkColumns[i].getName());
                }

                for (Column lobColumn : lobColumns) {
                    String sql = buildSelect(table, lobColumn, pkColumns);
                    String valueForCsv = null;
                    if (platform.isBlob(lobColumn.getMappedTypeCode())) {
                        byte[] binaryData = sqlTemplate.queryForBlob(sql, lobColumn.getJdbcTypeCode(),lobColumn.getJdbcTypeName(), args);
                        if (binaryData != null) {
                            if (batch.getBinaryEncoding() == BinaryEncoding.BASE64) {
                                valueForCsv = new String(Base64.encodeBase64(binaryData));
                            } else if (batch.getBinaryEncoding() == BinaryEncoding.HEX) {
                                valueForCsv = new String(Hex.encodeHex(binaryData));
                            } else {
                                valueForCsv = new String(binaryData);
                            }
                            binaryData = null;
                        }
                    } else {
                        valueForCsv = sqlTemplate.queryForClob(sql, lobColumn.getJdbcTypeCode(),lobColumn.getJdbcTypeName(), args);
                    }

                    int index = ArrayUtils.indexOf(columnNames, lobColumn.getName());
                    rowData[index] = valueForCsv;

                }

                data.putParsedData(CsvData.ROW_DATA, rowData);
            }
        }
        return data;
    }

    protected String buildSelect(Table table, Column lobColumn, Column[] pkColumns) {
        StringBuilder sql = new StringBuilder("select ");
        DatabaseInfo dbInfo = platform.getDatabaseInfo();
        String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? dbInfo.getDelimiterToken() : "";
        
        if ("XMLTYPE".equalsIgnoreCase(lobColumn.getJdbcTypeName()) && 2009 == lobColumn.getJdbcTypeCode()) {
            sql.append("extract(");
            sql.append(quote);
            sql.append(lobColumn.getName());
            sql.append(quote);
            sql.append(", '/').getClobVal()");
        } else {
            sql.append(quote);
            sql.append(lobColumn.getName());
            sql.append(quote);
        }
        sql.append(",");
        sql.delete(sql.length() - 1, sql.length());
        sql.append(" from ");
        sql.append(table.getQualifiedTableName(quote, dbInfo.getCatalogSeparator(), 
                dbInfo.getSchemaSeparator()));
        sql.append(" where ");
        for (Column col : pkColumns) {
            sql.append(quote);
            sql.append(col.getName());
            sql.append(quote);
            sql.append("=? and ");
        }
        sql.delete(sql.length() - 5, sql.length());
        return sql.toString();
    }

}
