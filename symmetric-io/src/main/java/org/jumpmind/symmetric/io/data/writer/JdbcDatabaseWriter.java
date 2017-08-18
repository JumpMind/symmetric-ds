package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;

public class JdbcDatabaseWriter extends DefaultDatabaseWriter {

	public JdbcDatabaseWriter(IDatabasePlatform platform) {
		super(platform);
	}

    public ISqlTransaction getTransaction() {
        return null;
    }
    
    @Override
    public void start(Batch batch) {
    		super.start(batch);
    }
    
    @Override
    public boolean start(Table table) {
    		return super.start(table);
    }
    
    @Override
    protected LoadStatus insert(CsvData data) {
    		return super.insert(data);
    }

    
    @Override
    public void write(CsvData data) {
    		super.write(data);
    }
}
