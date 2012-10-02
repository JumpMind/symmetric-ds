package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataProcessor;

public interface IDatabaseWriterErrorHandler extends IExtensionPoint {
    
    /**
     * This method is called if any error occurs while the {@link DatabaseWriter} is processing
     * a {@link CsvData} in the write method.  This method gives an option to take an action on
     * an error or even simply ignore it.
     * @param table TODO
     * 
     * @return true if the error should be processed as normal or false if the error should be ignored
     * and the {@link DataProcessor} should continue to process on.
     */
    public boolean handleError(DataContext context, Table table, CsvData data, Exception ex);

}
