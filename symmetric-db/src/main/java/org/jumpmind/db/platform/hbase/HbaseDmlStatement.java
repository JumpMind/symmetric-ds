package org.jumpmind.db.platform.hbase;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

public class HbaseDmlStatement extends DmlStatement {

    public HbaseDmlStatement(DmlType type, String catalogName, String schemaName, String tableName,
            Column[] keysColumns, Column[] columns, boolean[] nullKeyValues, 
            DatabaseInfo databaseInfo, boolean useQuotedIdentifiers, String textColumnExpression) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, 
                nullKeyValues, databaseInfo, useQuotedIdentifiers, textColumnExpression);
    }
    
    @Override
    public String buildInsertSql(String tableName, Column[] keyColumns, Column[] columns) {
        return super.buildInsertSql(tableName, keyColumns, columns).replaceAll("insert into", "upsert into");
    }
}
