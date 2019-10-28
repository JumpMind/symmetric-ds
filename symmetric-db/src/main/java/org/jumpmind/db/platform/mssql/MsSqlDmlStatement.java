package org.jumpmind.db.platform.mssql;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.sql.DmlStatement;

public class MsSqlDmlStatement extends DmlStatement {

    public MsSqlDmlStatement(DmlType type, String catalogName, String schemaName, String tableName, Column[] keysColumns, Column[] columns,
            boolean[] nullKeyValues, DatabaseInfo databaseInfo, boolean useQuotedIdentifiers, String textColumnExpression) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, nullKeyValues, databaseInfo, useQuotedIdentifiers,
                textColumnExpression);
    }

    public MsSqlDmlStatement(DmlType type, String catalogName, String schemaName, String tableName, Column[] keysColumns, Column[] columns,
            boolean[] nullKeyValues, DatabaseInfo databaseInfo, boolean useQuotedIdentifiers, String textColumnExpression,
            boolean namedParameters) {
        super(type, catalogName, schemaName, tableName, keysColumns, columns, nullKeyValues, databaseInfo, useQuotedIdentifiers,
                textColumnExpression, namedParameters);
    }

    @Override
    protected int getTypeCode(Column column, boolean isDateOverrideToTimestamp) {
        int type = column.getMappedTypeCode();
        if (type == Types.FLOAT) {
            return Types.VARCHAR;
        } else {
            return super.getTypeCode(column, isDateOverrideToTimestamp);
        }
    }
    
    @Override
    protected void appendColumnParameter(StringBuilder sql, Column column) {
        if (column.getJdbcTypeName() != null && column.getJdbcTypeName().equals("datetime2") && column.getMappedTypeCode() == Types.VARCHAR) {
            sql.append("cast(? AS datetime2(6))").append(",");
        } else {
            super.appendColumnParameter(sql, column);
        }
    }
    
    @Override
    protected void appendColumnEquals(StringBuilder sql, Column column) {
        if (column.getJdbcTypeName() != null && column.getJdbcTypeName().equals("datetime2") && column.getMappedTypeCode() == Types.VARCHAR) {
            sql.append(quote).append(column.getName()).append(quote)
            .append(" = cast(? AS datetime2(6))");
        } else {
            super.appendColumnEquals(sql, column);
        }
    }
}
