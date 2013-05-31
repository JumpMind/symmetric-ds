package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.model.Table;


public interface IDataReader extends IDataResource {

    public Batch nextBatch();

    public Table nextTable();

    public CsvData nextData();

}
