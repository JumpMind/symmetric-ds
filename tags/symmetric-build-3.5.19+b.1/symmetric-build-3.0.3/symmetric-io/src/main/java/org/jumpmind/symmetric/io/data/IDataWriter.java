package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.model.Table;


public interface IDataWriter extends IDataResource {

    public void start(Batch batch);

    public boolean start(Table table);

    public void write(CsvData data);

    public void end(Table table);

    public void end(Batch batch, boolean inError);

}
