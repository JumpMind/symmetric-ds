package org.jumpmind.db.platform.mysql;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.sql.DmlStatement;

public class MySqlDmlStatement extends DmlStatement {

    public MySqlDmlStatement(DmlType type, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, boolean isDateOverrideToTimestamp,
            String identifierQuoteString, boolean[] nullKeyValues) {
        super(type, catalogName, schemaName, tableName, keys, columns, isDateOverrideToTimestamp,
                identifierQuoteString, nullKeyValues);
    }

    @Override
    public void appendColumnQuestions(StringBuilder sql, Column[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (columns[i].getJdbcTypeName() != null && columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append("geomfromtext(?)").append(",");
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
                } else if (columns[i].getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY)) {
                    sql.append(quote).append(columns[i].getName()).append(quote).append(" = ")
                            .append("geomfromtext(?)").append(separator);
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
    
}
