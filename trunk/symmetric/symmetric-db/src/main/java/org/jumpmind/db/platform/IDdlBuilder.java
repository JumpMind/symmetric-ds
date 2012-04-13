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
    
    /*
     * Determines whether delimited identifiers are used or normal SQL92
     * identifiers (which may only contain alphanumerical characters and the
     * underscore, must start with a letter and cannot be a reserved keyword).
     * Per default, delimited identifiers are not used
     * 
     * @return <code>true</code> if delimited identifiers are used
     */
    public boolean isDelimitedIdentifierModeOn();


}
