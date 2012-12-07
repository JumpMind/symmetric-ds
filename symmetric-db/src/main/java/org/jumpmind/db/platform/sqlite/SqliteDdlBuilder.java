package org.jumpmind.db.platform.sqlite;

import java.sql.Connection;
import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;

public class SqliteDdlBuilder extends AbstractDdlBuilder {
    
    public SqliteDdlBuilder() {
        databaseInfo.setMinIsolationLevelToPreventPhantomReads(Connection.TRANSACTION_SERIALIZABLE);
        databaseInfo.setPrimaryKeyEmbedded(true);
        databaseInfo.setNonPKIdentityColumnsSupported(false);
        databaseInfo.setIdentityOverrideAllowed(false);
        databaseInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        databaseInfo.setNullAsDefaultValueRequired(false);
        databaseInfo.setRequiresAutoCommitForDdl(true);

        databaseInfo.addNativeTypeMapping(Types.ARRAY, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.CHAR, "VARCHAR", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(Types.BIGINT, "INTEGER", Types.INTEGER);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.NULL, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.REF, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "BINARY", Types.BINARY);
        databaseInfo.addNativeTypeMapping(Types.DATALINK, "BINARY", Types.BINARY);
        
        databaseInfo.addNativeTypeMapping(Types.TIMESTAMP, "TIMESTAMP",Types.TIMESTAMP);
        databaseInfo.addNativeTypeMapping(Types.TIME, "TIME", Types.TIME);
        databaseInfo.addNativeTypeMapping(Types.DATE, "DATETIME", Types.DATE);

        databaseInfo.addNativeTypeMapping(Types.BIT, "INTEGER", Types.INTEGER);
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "INTEGER", Types.INTEGER);
        databaseInfo.addNativeTypeMapping(Types.SMALLINT, "INTEGER", Types.INTEGER);
        databaseInfo.addNativeTypeMapping(Types.BINARY, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        databaseInfo.addNativeTypeMapping(Types.CLOB, "VARCHAR", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "VARCHAR", Types.VARCHAR);
        databaseInfo.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.FLOAT);
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        databaseInfo.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        databaseInfo.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        databaseInfo.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        databaseInfo.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);

        databaseInfo.setHasSize(Types.CHAR, false);
        databaseInfo.setHasSize(Types.VARCHAR, false);
        databaseInfo.setHasSize(Types.BINARY, false);
        databaseInfo.setHasSize(Types.VARBINARY, false);

        databaseInfo.setHasPrecisionAndScale(Types.DECIMAL, false);
        databaseInfo.setHasPrecisionAndScale(Types.NUMERIC, false);

        databaseInfo.setDateOverridesToTimestamp(false);
        databaseInfo.setEmptyStringNulled(false);
        databaseInfo.setBlankCharColumnSpacePadded(false);
        databaseInfo.setNonBlankCharColumnSpacePadded(false);
        databaseInfo.setForeignKeysSupported(false);
    }
    
    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        ddl.append("DROP INDEX ");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }
    
    /**
     * Prints that the column is an auto increment column.
     */
    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        ddl.append("AUTOINCREMENT");
    }    
    
    @Override
    protected void writeColumnEmbeddedPrimaryKey(Table table, Column column, StringBuilder ddl) {
        if (table.getPrimaryKeyColumnCount() == 1) {
            ddl.append(" PRIMARY KEY ");
        }
    }
    
    @Override
    protected void writeEmbeddedPrimaryKeysStmt(Table table, StringBuilder ddl) {
        if (table.getPrimaryKeyColumnCount() > 1) {
        super.writeEmbeddedPrimaryKeysStmt(table, ddl);
        }
    }
}
