package org.jumpmind.db.platform;


import java.util.List;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;

public interface IDdlReader {

    public Database readTables(String catalog, String schema, String[] tableTypes);

    public Table readTable(String catalog, String schema, String tableName);
    
    public List<String> getCatalogs();
    
    public List<String> getSchemas(String catalog);
    
}
