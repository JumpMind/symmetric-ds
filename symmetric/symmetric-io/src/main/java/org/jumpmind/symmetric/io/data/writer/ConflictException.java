package org.jumpmind.symmetric.io.data.writer;

import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;

public class ConflictException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    protected CsvData data;

    protected Table table;

    protected boolean fallbackOperationFailed = false;

    public ConflictException(CsvData data, Table table, boolean fallbackOperationFailed) {
        super(message(data, table, fallbackOperationFailed));
        this.data = data;
        this.table = table;
        this.fallbackOperationFailed = fallbackOperationFailed;
    }

    protected static String message(CsvData data, Table table, boolean fallbackOperationFailed) {
        Map<String, String> pks = data.toColumnNameValuePairs(table.getPrimaryKeyColumnNames(),
                CsvData.PK_DATA);
        if (pks == null || pks.size() == 0) {
            pks = data.toColumnNameValuePairs(table.getPrimaryKeyColumnNames(), CsvData.OLD_DATA);
        }

        if (pks == null || pks.size() == 0) {
            pks = data.toColumnNameValuePairs(table.getPrimaryKeyColumnNames(), CsvData.ROW_DATA);
        }

        return String.format(
                "Detected conflict while executing %s on %s.  The primary key data was: %s. %s",
                data.getDataEventType().toString(), table.getFullyQualifiedTableName(), pks,
                fallbackOperationFailed ? "Failed to fallback." : "");
    }

    public CsvData getData() {
        return data;
    }

    public Table getTable() {
        return table;
    }

    public boolean isFallbackOperationFailed() {
        return fallbackOperationFailed;
    }

}
