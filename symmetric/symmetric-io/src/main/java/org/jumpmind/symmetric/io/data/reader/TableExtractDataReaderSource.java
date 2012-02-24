package org.jumpmind.symmetric.io.data.reader;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;

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
            String schemaName, String tableName, String whereClause, boolean streamLobs) {
        this.platform = platform;
        this.table = platform.getTableFromCache(catalogName, schemaName, tableName, true);
        if (table == null) {
            throw new IllegalStateException(String.format("Could not find table %s",
                    Table.getFullyQualifiedTableName(catalogName, schemaName, tableName)));
        }
        this.whereClause = whereClause;
        this.streamLobs = streamLobs;
        this.batch = new Batch(-1, "default", BinaryEncoding.BASE64, "localhost");

    }

    public Batch getBatch() {
        return this.batch;
    }

    public Table getTable() {
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
                .getPlatformInfo().getIdentifierQuoteString()),
                StringUtils.isNotBlank(whereClause) ? " where " + whereClause : "");
        this.cursor = platform.getSqlTemplate().queryForCursor(sql, new ISqlRowMapper<CsvData>() {
            public CsvData mapRow(Row row) {
                return new CsvData(DataEventType.INSERT, toPkData(row), toRowData(row));
            }
        });
    }

    protected String[] toRowData(Row row) {
        return null;
    }

    protected String[] toPkData(Row row) {
        return null;
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
