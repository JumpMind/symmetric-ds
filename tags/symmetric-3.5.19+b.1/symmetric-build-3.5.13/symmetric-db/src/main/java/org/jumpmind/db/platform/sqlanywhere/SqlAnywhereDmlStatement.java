package org.jumpmind.db.platform.sqlanywhere;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

public class SqlAnywhereDmlStatement extends DmlStatement {

    public SqlAnywhereDmlStatement(DmlType type, String catalogName,
            String schemaName, String tableName, Column[] keysColumns,
            Column[] columns, boolean[] nullKeyValues,
            DatabaseInfo databaseInfo, boolean useQuotedIdentifiers) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns,
                nullKeyValues, databaseInfo, useQuotedIdentifiers);
    }

    @Override
    protected int getTypeCode(Column column, boolean isDateOverrideToTimestamp) {
        int type = column.getMappedTypeCode();
        if (type == Types.DATE && isDateOverrideToTimestamp) {
            type = Types.TIMESTAMP;
        }
        return type;
    }

}
