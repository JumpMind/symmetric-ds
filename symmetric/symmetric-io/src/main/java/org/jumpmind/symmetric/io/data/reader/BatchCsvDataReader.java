package org.jumpmind.symmetric.io.data.reader;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;

public class BatchCsvDataReader implements IDataReader {

    protected String selectSql;

    protected IDatabasePlatform platform;

    protected ISqlReadCursor<CsvData> dataCursor;

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected List<Batch> batchesToSend;

    protected Map<Long, CsvDataSettings> csvDataSettings;

    protected Batch batch;

    protected CsvDataSettings csvDataSetting;

    protected CsvData data;

    protected boolean extractOldData = true;

    public BatchCsvDataReader(IDatabasePlatform platform, String sql,
            Map<Long, CsvDataSettings> csvDataSettings, boolean extractOldData, Batch... batches) {
        this.selectSql = sql;
        this.extractOldData = extractOldData;
        this.platform = platform;
        this.csvDataSettings = csvDataSettings;
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
        csvDataSetting = null;
        data = this.dataCursor.next();
        if (data != null) {
            csvDataSetting = csvDataSettings.get(data.getAttribute(CsvData.ATTRIBUTE_TABLE_ID));
            if (csvDataSetting == null) {
                throw new RuntimeException(String.format(
                        "Table mapping for id of %d was not found",
                        data.getAttribute(CsvData.ATTRIBUTE_TABLE_ID)));
            }
        }
        return csvDataSetting != null ? csvDataSetting.getTableMetaData() : null;
    }

    public CsvData nextData() {
        if (data == null) {
            data = this.dataCursor.next();
        }

        CsvData returnData = null;
        if (data != null) {
            CsvDataSettings newCsvDataSetting = csvDataSettings.get(data
                    .getAttribute(CsvData.ATTRIBUTE_TABLE_ID));
            if (newCsvDataSetting != null
                    && csvDataSetting != null
                    && newCsvDataSetting.getTableMetaData().equals(
                            csvDataSetting.getTableMetaData())) {
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

    protected String enhanceWithLobsFromTargetIfNeeded(String rowData) {
        // TODO
        return rowData;
    }

    class CsvDataRowMapper implements ISqlRowMapper<CsvData> {
        public CsvData mapRow(Row row) {
            CsvData data = new CsvData();
            String rowData = row.getString("ROW_DATA");
            if (rowData != null && csvDataSetting.isSelectLobsFromTarget()) {
                rowData = enhanceWithLobsFromTargetIfNeeded(rowData);
            }
            data.putCsvData(CsvData.ROW_DATA, rowData);
            data.putCsvData(CsvData.PK_DATA, row.getString("PK_DATA"));
            if (extractOldData) {
                data.putCsvData(CsvData.OLD_DATA, row.getString("OLD_DATA"));
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

    public static class CsvDataSettings {
        protected boolean selectLobsFromTarget = false;
        protected Table tableMetaData;

        public CsvDataSettings(boolean useStreamLobs, Table tableMetaData) {
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
