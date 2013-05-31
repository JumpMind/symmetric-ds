package org.jumpmind.symmetric.io.data.reader;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.Batch.BatchType;

/**
 * Convert a source table's rows to {@link CsvData}
 */
public class TableExtractDataReaderSource implements IExtractDataReaderSource {

    protected IDatabasePlatform platform;

    protected String whereClause;

    protected Batch batch;

    protected Table table;

    protected ISqlReadCursor<CsvData> cursor;

    protected boolean streamLobs;

    public TableExtractDataReaderSource(IDatabasePlatform platform, String catalogName,
            String schemaName, String tableName, String whereClause, boolean streamLobs, String sourceNodeId, String targetNodeId) {
        this.platform = platform;
        this.table = platform.getTableFromCache(catalogName, schemaName, tableName, true);
        if (table == null) {
            throw new IllegalStateException(String.format("Could not find table %s",
                    Table.getFullyQualifiedTableName(catalogName, schemaName, tableName)));
        }
        this.whereClause = whereClause;
        this.streamLobs = streamLobs;
        this.batch = new Batch(BatchType.EXTRACT, -1, "default", BinaryEncoding.BASE64, sourceNodeId, targetNodeId, false);

    }

    public Batch getBatch() {
        return this.batch;
    }

    public Table getTargetTable() {
        return this.table;
    }
   
    public Table getSourceTable() {
        return this.table;
    }

    public CsvData next() {
        CsvData data = null;
        if (cursor == null) {
            startNewCursor();
        }

        if (cursor != null) {
            data = cursor.next();
            if (data == null) {
                closeCursor();
            }
        }
        return data;
    }

    protected void startNewCursor() {
        String sql = String.format("select * from %s %s", table.getFullyQualifiedTableName(platform
                .getDatabaseInfo().getDelimiterToken()),
                StringUtils.isNotBlank(whereClause) ? " where " + whereClause : "");
        this.cursor = platform.getSqlTemplate().queryForCursor(sql, new ISqlRowMapper<CsvData>() {
            public CsvData mapRow(Row row) {
                return new CsvData(DataEventType.INSERT, toStringData(row, table.getPrimaryKeyColumns()), toStringData(row, table.getColumns()));
            }
        });
    }

    protected String[] toStringData(Row row, Column[] columns) {
        String[] stringValues = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {            
            Object value = row.get(columns[i].getName());
            if (value instanceof byte[]) {
                stringValues[i] = new String(Base64.encodeBase64((byte[])value));
            } else if (value != null) {
                stringValues[i] = value.toString();
            }
        }
        return stringValues;
    }

    public boolean requiresLobsSelectedFromSource() {
        return streamLobs;
    }

    public void close() {
        closeCursor();
    }

    protected void closeCursor() {
        if (this.cursor != null) {
            this.cursor.close();
            this.cursor = null;
        }
    }

}
