package org.jumpmind.symmetric.io.data.reader;

import java.sql.Types;
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
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.Statistics;

public class DatabaseCapturedCsvDataReader implements IDataReader {

    protected String selectSql;

    protected IDatabasePlatform platform;

    protected ISqlReadCursor<CsvData> dataCursor;

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected List<Batch> batchesToSend;

    protected Map<Long, DatabaseCaptureSettings> settings;

    protected Batch batch;

    protected DatabaseCaptureSettings setting;

    protected CsvData data;

    protected boolean extractOldData = true;

    public DatabaseCapturedCsvDataReader(IDatabasePlatform platform, String sql,
            Map<Long, DatabaseCaptureSettings> csvDataSettings, boolean extractOldData,
            Batch... batches) {
        this.selectSql = sql;
        this.extractOldData = extractOldData;
        this.platform = platform;
        this.settings = csvDataSettings;
        this.batchesToSend = new ArrayList<Batch>(batches.length);
        for (Batch batch : batches) {
            this.batchesToSend.add(batch);
        }
    }

    public <R extends IDataReader, W extends IDataWriter> void open(DataContext<R, W> context) {
    }

    public Batch nextBatch() {
        closeDataCursor();
        if (this.batchesToSend.size() > 0) {
            this.batch = this.batchesToSend.remove(0);
            this.statistics.put(batch, new Statistics());
            dataCursor = platform.getSqlTemplate().queryForCursor(selectSql,
                    new CsvDataRowMapper(), new Object[] { batch.getBatchId() },
                    new int[] { Types.NUMERIC });
            return batch;
        } else {
            this.batch = null;
            return null;
        }

    }

    protected void closeDataCursor() {
        if (this.dataCursor != null) {
            this.dataCursor.close();
            this.dataCursor = null;
        }
    }

    public Table nextTable() {
        setting = null;
        data = this.dataCursor.next();
        if (data != null) {
            setting = settings.get(data.getAttribute(CsvData.ATTRIBUTE_TABLE_ID));
            if (setting == null) {
                throw new RuntimeException(String.format(
                        "Table mapping for id of %d was not found",
                        data.getAttribute(CsvData.ATTRIBUTE_TABLE_ID)));
            }
        }
        return setting != null ? setting.getTableMetaData() : null;
    }

    public CsvData nextData() {
        if (data == null) {
            data = this.dataCursor.next();
        }

        CsvData returnData = null;
        if (data != null) {
            DatabaseCaptureSettings newCsvDataSetting = settings.get(data
                    .getAttribute(CsvData.ATTRIBUTE_TABLE_ID));
            if (newCsvDataSetting != null && setting != null
                    && newCsvDataSetting.getTableMetaData().equals(setting.getTableMetaData())) {
                returnData = data;
                data = null;
            }
        }
        return returnData;
    }

    public void close() {
        closeDataCursor();
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

    protected void enhanceWithLobsFromTargetIfNeeded(CsvData data) {
        Table table = setting.getTableMetaData();
        table = platform.getTableFromCache(table.getCatalog(), table.getSchema(), table.getName(),
                false);
        if (table != null) {
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

    class CsvDataRowMapper implements ISqlRowMapper<CsvData> {
        public CsvData mapRow(Row row) {
            CsvData data = new CsvData();
            data.putCsvData(CsvData.ROW_DATA, row.getString("ROW_DATA"));
            data.putCsvData(CsvData.PK_DATA, row.getString("PK_DATA"));
            if (extractOldData) {
                data.putCsvData(CsvData.OLD_DATA, row.getString("OLD_DATA"));
            }

            if (setting.isSelectLobsFromTarget()) {
                enhanceWithLobsFromTargetIfNeeded(data);
            }

            data.putAttribute(CsvData.ATTRIBUTE_CHANNEL_ID, row.getString("CHANNEL_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_TX_ID, row.getString("TRANSACTION_ID"));
            data.setDataEventType(DataEventType.getEventType(row.getString("EVENT_TYPE")));
            data.putAttribute(CsvData.ATTRIBUTE_TABLE_ID, row.getInt("TRIGGER_HIST_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_SOURCE_NODE_ID, row.getString("SOURCE_NODE_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_ROUTER_ID, row.getString("ROUTER_ID"));
            data.putAttribute(CsvData.ATTRIBUTE_EXTERNAL_DATA, row.getString("EXTERNAL_DATA"));
            data.putAttribute(CsvData.ATTRIBUTE_DATA_ID, row.getLong("DATA_ID"));
            return data;
        }
    }

    public static class DatabaseCaptureSettings {

        protected boolean selectLobsFromTarget = false;

        protected Table tableMetaData;

        public DatabaseCaptureSettings(boolean useStreamLobs, Table tableMetaData) {
            this.selectLobsFromTarget = useStreamLobs;
            this.tableMetaData = tableMetaData;
        }

        public boolean isSelectLobsFromTarget() {
            return selectLobsFromTarget;
        }

        public void setSelectLobsFromTarget(boolean selectLobsFromTarget) {
            this.selectLobsFromTarget = selectLobsFromTarget;
        }

        public Table getTableMetaData() {
            return tableMetaData;
        }

        public void setTableMetaData(Table tableMetaData) {
            this.tableMetaData = tableMetaData;
        }

    }

}
