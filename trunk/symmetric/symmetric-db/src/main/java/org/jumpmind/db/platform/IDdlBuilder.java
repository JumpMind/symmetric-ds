package org.jumpmind.db.platform;

import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;

public interface IDdlBuilder {
    
    public String createTables(Database database, boolean dropTables);
    
    public String getTableName(String tableName);
    
    public String getIndexName(IIndex index);
    
    public String getForeignKeyName(Table table, ForeignKey fk);

    public String getConstraintName(String prefix, Table table, String secondPart, String suffix);
    
    public boolean isAlterDatabase(Database currentModel, Database desiredModel);
    
    public String createTable(Table table);
    
    public String alterDatabase(Database currentModel, Database desiredModel);
    
    public String dropTables(Database database);

}
