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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.Statistics;

public class ExtractDataReader implements IDataReader {

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected IDatabasePlatform platform;

    protected List<IExtractDataReaderSource> sourcesToUse;

    protected IExtractDataReaderSource currentSource;

    protected Batch batch;

    protected Table table;

    protected CsvData data;

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
            }
        }
        
        if (this.table == null && this.batch != null) {
            this.batch.setComplete(true);
        }
        return this.table;
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
        if (this.currentSource.requiresLobsSelectedFromSource()
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
                        }
                    } else {
                        valueForCsv = sqlTemplate.queryForClob(sql, args);
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
        String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? platform
                .getDatabaseInfo().getDelimiterToken() : "";
        sql.append(quote);
        sql.append(lobColumn.getName());
        sql.append(quote);
        sql.append(",");
        sql.delete(sql.length() - 1, sql.length());
        sql.append(" from ");
        sql.append(table.getFullyQualifiedTableName(quote));
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
