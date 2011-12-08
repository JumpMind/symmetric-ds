package org.jumpmind.symmetric.io.data;


public interface IDataWriterErrorHandler {

    public boolean handleWriteError(Exception error, Batch batch, CsvData data, int dataRow);

}
