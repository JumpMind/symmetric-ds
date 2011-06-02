package org.jumpmind.symmetric.android.db.sqlite;

import java.util.List;

import org.jumpmind.symmetric.core.db.AbstractDbDialect;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;

import android.database.sqlite.SQLiteOpenHelper;

public class SQLiteDbDialect extends AbstractDbDialect {

    protected SQLiteOpenHelper openHelper;

    public SQLiteDbDialect(SQLiteOpenHelper openHelper, Parameters parameters) {
        super(parameters);
        this.openHelper = openHelper;
    }

    public String getAlterScriptFor(Table... tables) {
        // TODO Auto-generated method stub
        return null;
    }

    public Database findDatabase(String catalogName, String schemaName) {
        // TODO Auto-generated method stub
        return null;
    }

    public Table findTable(String tableName) {
        // TODO Auto-generated method stub
        return null;
    }

    public Table findTable(String catalogName, String schemaName, String tableName,
            boolean useCached) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Table> findTables(String catalogName, String schemaName) {
        // TODO Auto-generated method stub
        return null;
    }

    public ISqlTemplate getSqlConnection() {
        // TODO Auto-generated method stub
        return null;
    }

    public boolean isDataIntegrityException(Exception ex) {
        // TODO Auto-generated method stub
        return false;
    }

    public boolean supportsBatchUpdates() {
        // TODO Auto-generated method stub
        return false;
    }

}
