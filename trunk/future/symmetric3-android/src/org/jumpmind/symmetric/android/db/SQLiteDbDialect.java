package org.jumpmind.symmetric.android.db;

import java.sql.Types;
import java.util.List;

import org.jumpmind.symmetric.core.db.AbstractDbDialect;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;

import android.database.sqlite.SQLiteOpenHelper;

public class SQLiteDbDialect extends AbstractDbDialect {

    protected SQLiteOpenHelper openHelper;
    
    protected SQLiteSqlTemplate sqlTemplate;

    public SQLiteDbDialect(SQLiteOpenHelper openHelper, Parameters parameters) {
        super(parameters);
        
        dialectInfo.setNonPKIdentityColumnsSupported(false);
        dialectInfo.setIdentityOverrideAllowed(false);
        dialectInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        dialectInfo.setNullAsDefaultValueRequired(false);
        dialectInfo.addNativeTypeMapping(Types.ARRAY, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DISTINCT, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.NULL, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.REF, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.STRUCT, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DATALINK, "BINARY", Types.BINARY);

        dialectInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);
        dialectInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.TINYINT);
        dialectInfo.addNativeTypeMapping(Types.SMALLINT, "SMALLINT", Types.SMALLINT);
        dialectInfo.addNativeTypeMapping(Types.BINARY, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.CLOB, "CLOB", Types.CLOB);
        dialectInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        dialectInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        dialectInfo.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);
        
        this.openHelper = openHelper;
        
        dialectInfo.setDateOverridesToTimestamp(false);
        dialectInfo.setEmptyStringNulled(false);
        dialectInfo.setBlankCharColumnSpacePadded(true);
        dialectInfo.setNonBlankCharColumnSpacePadded(false);
        dialectInfo.setRequiresAutoCommitFalseToSetFetchSize(false);

        this.tableBuilder = new SQLiteTableBuilder(this);
        this.dataCaptureBuilder = new SQLiteDataCaptureBuilder(this);
    }

    public String getAlterScriptFor(Table... tables) {
        // TODO Auto-generated method stub
        return null;
    }

    public Database findDatabase(String catalogName, String schemaName) {
        // TODO Auto-generated method stub
        return null;
    }

    // http://www.xerial.org/trac/Xerial/browser/XerialJ/trunk/sqlite-jdbc/src/main/java/org/sqlite/MetaData.java
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
        return false;
    }

    public boolean supportsBatchUpdates() {
        return true;
    }

}
