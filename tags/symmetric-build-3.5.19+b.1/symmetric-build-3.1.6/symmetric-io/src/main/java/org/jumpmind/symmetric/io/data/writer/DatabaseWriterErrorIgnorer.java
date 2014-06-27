package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;

/**
 * An {@link IDatabaseWriterErrorHandler} that ignores all errors.
 */
public class DatabaseWriterErrorIgnorer implements IDatabaseWriterErrorHandler {

    public boolean handleError(DataContext context, Table table, CsvData data, Exception ex) {
        return false;
    }

}
