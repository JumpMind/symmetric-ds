/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.load;

import java.sql.Types;
import java.util.ArrayList;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.ddlutils.model.Column;
import org.jumpmind.symmetric.db.BinaryEncoding;

public class StatementBuilder {
    public enum DmlType {
        INSERT, UPDATE, DELETE
    };

    protected DmlType dmlType;

    protected String sql;
    
    protected int[] types;

    public StatementBuilder(DmlType type, String tableName, String[] keyNames, String[] columnNames) {
        if (type == DmlType.INSERT) {
            sql = buildInsertSql(tableName, columnNames);
        } else if (type == DmlType.UPDATE) {
            sql = buildUpdateSql(tableName, keyNames, columnNames);
        } else if (type == DmlType.DELETE) {
            sql = buildDeleteSql(tableName, keyNames);
        } else {
            throw new NotImplementedException("Unimplemented SQL type: " + type);
        }
        dmlType = type;
    }

    public StatementBuilder(DmlType type, String tableName, Column[] keys, Column[] columns, BinaryEncoding encoding) {
        if (type == DmlType.INSERT) {
            sql = buildInsertSql(tableName, columns);
            types = buildTypes(columns, encoding);
        } else if (type == DmlType.UPDATE) {
            sql = buildUpdateSql(tableName, keys, columns);
            types = buildTypes(keys, columns, encoding);
        } else if (type == DmlType.DELETE) {
            sql = buildDeleteSql(tableName, keys);
            types = buildTypes(keys, encoding);
        } else {
            throw new NotImplementedException("Unimplemented SQL type: " + type);
        }
        dmlType = type;
    }

    protected int[] buildTypes(Column[] keys, Column[] columns, BinaryEncoding encoding) {
        int[] columnTypes = buildTypes(columns, encoding);
        int[] keyTypes = buildTypes(keys, encoding);
        return ArrayUtils.addAll(columnTypes, keyTypes);
    }
    
    protected int[] buildTypes(Column[] columns, BinaryEncoding encoding) {
        ArrayList<Integer> list = new ArrayList<Integer>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                list.add(columns[i].getTypeCode());
            }
        }
        int[] types = new int[list.size()];
        int index = 0;
        for (Integer type : list) {
            if (type == Types.BLOB && encoding == BinaryEncoding.NONE) {
                type = Types.BINARY;
            }
            types[index++] = type;
        }
        return types;
    }
    
    public static String buildInsertSql(String tableName, String[] columnNames) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + "(");
        appendColumns(sql, columnNames);
        sql.append(") values (");
        appendColumnQuestions(sql, columnNames.length);
        sql.append(")");
        return sql.toString();
    }

    public static String buildInsertSql(String tableName, Column[] columns) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + "(");
        int columnCount = appendColumns(sql, columns);
        sql.append(") values (");
        appendColumnQuestions(sql, columnCount);
        sql.append(")");
        return sql.toString();
    }

    public static String buildUpdateSql(String tableName, String[] keyNames, String[] columnNames) {
        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
        appendColumnEquals(sql, columnNames, ", ");
        sql.append(" where ");
        appendColumnEquals(sql, keyNames, " and ");
        return sql.toString();
    }

    public static String buildUpdateSql(String tableName, Column[] keyColumns, Column[] columns) {
        StringBuilder sql = new StringBuilder("update ").append(tableName).append(" set ");
        appendColumnEquals(sql, columns, ", ");
        sql.append(" where ");
        appendColumnEquals(sql, keyColumns, " and ");
        return sql.toString();
    }

    public static String buildDeleteSql(String tableName, String[] keyNames) {
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyNames, " and ");
        return sql.toString();
    }

    public static String buildDeleteSql(String tableName, Column[] keyColumns) {
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyColumns, " and ");
        return sql.toString();
    }

    public static void appendColumnEquals(StringBuilder sql, String[] names, String separator) {
        for (int i = 0; i < names.length; i++) {
            sql.append(names[i]).append(" = ?").append(i + 1 < names.length ? separator : "");
        }
    }

    public static void appendColumnEquals(StringBuilder sql, Column[] columns, String separator) {
        int existingCount = 0;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (existingCount++ > 0) {
                    sql.append(separator);
                }
                sql.append(columns[i].getName()).append(" = ?");
            }
        }
    }

    public static void appendColumns(StringBuilder sql, String[] names) {
        for (int i = 0; i < names.length; i++) {
            sql.append(names[i]).append(i + 1 < names.length ? "," : "");
        }
    }

    public static int appendColumns(StringBuilder sql, Column[] columns) {
        int existingCount = 0;
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                if (existingCount++ > 0) {
                    sql.append(",");
                }
                sql.append(columns[i].getName());
            }
        }
        return existingCount;
    }

    public static void appendColumnQuestions(StringBuilder sql, int number) {
        for (int i = 0; i < number; i++) {
            sql.append("?").append(i + 1 < number ? "," : "");
        }
    }

    public String getSql() {
        return sql;
    }

    public DmlType getDmlType() {
        return dmlType;
    }
    
    public int[] getTypes() {
        return types;
    }
}
