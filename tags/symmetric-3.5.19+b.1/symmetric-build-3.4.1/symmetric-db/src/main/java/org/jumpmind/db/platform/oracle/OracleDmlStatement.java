package org.jumpmind.db.platform.oracle;

import java.sql.Types;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.sql.DmlStatement;

public class OracleDmlStatement extends DmlStatement {

    public OracleDmlStatement(DmlType type, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, boolean isDateOverrideToTimestamp,
            String identifierQuoteString, boolean[] nullKeyValues) {
        super(type, catalogName, schemaName, tableName, keys, columns, isDateOverrideToTimestamp,
                identifierQuoteString, nullKeyValues);
    }

    @Override
    public void appendColumnQuestions(StringBuilder sql, Column[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (columns[i].getMappedTypeCode() == -101) {
                    sql.append("TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')")
                            .append(",");
                } else if (columns[i].getJdbcTypeName() != null && columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append("SYM_WKT2GEOM(?)").append(",");
                } else {
                    sql.append("?").append(",");
                }
            }
        }

        if (columns.length > 0) {
            sql.replace(sql.length() - 1, sql.length(), "");
        }
    }

    @Override
    public void appendColumnEquals(StringBuilder sql, Column[] columns, boolean[] nullValues, String separator) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (nullValues[i]) {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" is NULL")
                            .append(separator);
                } else if (columns[i].getMappedTypeCode() == -101) {
                    sql.append(quote).append(columns[i].getName()).append(quote)
                            .append(" = TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')")
                            .append(separator);
                } else if (columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" = ")
                            .append("SYM_WKT2GEOM(?)").append(separator);
                } else {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" = ?")
                            .append(separator);
                }
            }
        }

        if (columns.length > 0) {
            sql.replace(sql.length() - separator.length(), sql.length(), "");
        }
    }
    
    @Override
    protected int getTypeCode(Column column, boolean isDateOverrideToTimestamp) {
        int typeCode = super.getTypeCode(column, isDateOverrideToTimestamp);
        if (typeCode == Types.LONGVARCHAR) {
            typeCode = Types.CLOB;
        } 
        return typeCode;
    }
    
    @Override
    protected void appendColumnNameForSql(StringBuilder sql, Column column, boolean select) {
        String columnName = column.getName();
        if (select && column.isTimestampWithTimezone()) {
            sql.append("to_char(").append(quote).append(columnName).append(quote).append(", 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM') as ").append(columnName);
        } else {
            super.appendColumnNameForSql(sql, column, select);
        }        
    }


}
