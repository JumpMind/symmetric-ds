package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public interface IDatabaseWriterFilter extends IExtensionPoint {

    /**
     * Called before a DML statement will be executed against the database for
     * the data.
     * 
     * @return true if the row should be loaded. false if the filter has handled
     *         the row and it should be ignored.
     */
    public <R extends IDataReader, W extends IDataWriter> boolean beforeWrite(
            DataContext context, Table table, CsvData data);

    /**
     * Called right after a DML statement has been successfully executed against
     * the database for the data.
     */
    public <R extends IDataReader, W extends IDataWriter> void afterWrite(
            DataContext context, Table table, CsvData data);

    /**
     * Give the filter a chance to indicate that is can handle a table that is
     * missing. This might return true if the filter will be performing
     * transformations on the data and inserting the data itself.
     */
    public <R extends IDataReader, W extends IDataWriter> boolean handlesMissingTable(
            DataContext context, Table table);

    /**
     * If the {@link ParameterConstants#DATA_LOADER_MAX_ROWS_BEFORE_COMMIT}
     * property is set and the max number of rows is reached and a commit is
     * about to happen, then this method is called.
     */
    public <R extends IDataReader, W extends IDataWriter> void earlyCommit(
            DataContext context);

    /**
     * This method is called after a batch has been successfully processed. It
     * is called in the scope of the transaction that controls the batch commit.
     */
    public <R extends IDataReader, W extends IDataWriter> void batchComplete(
            DataContext context);

    /**
     * This method is called after the database transaction for the batch has
     * been committed.
     */
    public <R extends IDataReader, W extends IDataWriter> void batchCommitted(
            DataContext context);

    /**
     * This method is called after the database transaction for the batch has
     * been rolled back.
     */
    public <R extends IDataReader, W extends IDataWriter> void batchRolledback(
            DataContext context);

}
