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
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.Statistics;

public class ExtractDataReader implements IDataReader {

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected IDatabasePlatform platform;

    protected List<IExtractBatchSource> sourcesToUse;

    protected IExtractBatchSource currentSource;

    protected Batch batch;

    protected Table table;

    protected CsvData data;
    
    public ExtractDataReader(IDatabasePlatform platform, IExtractBatchSource source) {        
        this.sourcesToUse = new ArrayList<IExtractBatchSource>();
        this.sourcesToUse.add(source);
        this.platform = platform;
    }

    public ExtractDataReader(IDatabasePlatform platform, List<IExtractBatchSource> sources) {
        this.sourcesToUse = new ArrayList<IExtractBatchSource>(sources);
        this.platform = platform;
    }

    public <R extends IDataReader, W extends IDataWriter> void open(DataContext<R, W> context) {
    }

    public Batch nextBatch() {
        closeCurrentSource();
        if (this.sourcesToUse.size() > 0) {
            this.currentSource = this.sourcesToUse.get(0);
            this.batch = this.currentSource.getBatch();
        }

        return this.batch;

    }

    public Table nextTable() {
        this.table = null;
        if (this.data == null) {
            this.data = this.currentSource.next();
        }
        if (this.data != null) {
            this.table = this.currentSource.getTable();
        }
        return this.table;
    }

    public CsvData nextData() {
        if (this.data == null) {
            this.data = this.currentSource.next();
        }

        if (data == null) {
            closeCurrentSource();
        } else {
            Table sourceTable = this.currentSource.getTable();
            if (sourceTable != null && sourceTable.equals(this.table)) {
                return enhanceWithLobsFromSourceIfNeeded(table, data);
            }
        }

        return null;
    }

    public void close() {
        closeCurrentSource();
    }

    protected void closeCurrentSource() {
        if (this.currentSource != null) {
            this.currentSource.close();
            this.currentSource = null;
        }

        this.batch = null;
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
                    if (platform.isBlob(lobColumn.getTypeCode())) {
                        byte[] binaryData = sqlTemplate.queryForBlob(sql, args);
                        if (batch.getBinaryEncoding() == BinaryEncoding.BASE64) {
                            valueForCsv = new String(Base64.encodeBase64(binaryData));
                        } else if (batch.getBinaryEncoding() == BinaryEncoding.HEX) {
                            valueForCsv = new String(Hex.encodeHex(binaryData));
                        } else {
                            valueForCsv = new String(binaryData);
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
        String quote = platform.getPlatformInfo().getIdentifierQuoteString();
        sql.append(quote);
        sql.append(lobColumn.getName());
        sql.append(quote);
        sql.append(",");
        sql.delete(sql.length() - 1, sql.length());
        sql.append(" from ");
        sql.append(table.getFullyQualifiedTableName());
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
