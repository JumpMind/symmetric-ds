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

import org.apache.commons.lang.NotImplementedException;

public class StatementBuilder {
    public enum DmlType {
        INSERT, UPDATE, DELETE
    };

    protected DmlType type;

    protected String sql;

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
    }

    public static String buildInsertSql(String tableName, String[] columnNames) {
        StringBuilder sql = new StringBuilder("insert into " + tableName + "(");
        appendColumns(sql, columnNames);
        sql.append(") values (");
        appendColumnQuestions(sql, columnNames.length);
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

    public static String buildDeleteSql(String tableName, String[] keyNames) {
        StringBuilder sql = new StringBuilder("delete from ").append(tableName).append(" where ");
        appendColumnEquals(sql, keyNames, " and ");
        return sql.toString();
    }

    public static void appendColumnEquals(StringBuilder sql, String[] names, String separator) {
        for (int i = 0; i < names.length; i++) {
            sql.append(names[i]).append(" = ?").append(i + 1 < names.length ? separator : "");
        }
    }

    public static void appendColumns(StringBuilder sql, String[] names) {
        for (int i = 0; i < names.length; i++) {
            sql.append(names[i]).append(i + 1 < names.length ? "," : "");
        }
    }

    public static void appendColumnQuestions(StringBuilder sql, int number) {
        for (int i = 0; i < number; i++) {
            sql.append("?").append(i + 1 < number ? "," : "");
        }
    }

    public String getSql() {
        return sql;
    }

}
