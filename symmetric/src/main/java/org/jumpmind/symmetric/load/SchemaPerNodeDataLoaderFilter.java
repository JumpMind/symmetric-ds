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

import org.jumpmind.symmetric.db.IDbDialect;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * An extension that prefixes the table name with a schema name that is equal to the incoming node_id.
 */
public class SchemaPerNodeDataLoaderFilter implements IDataLoaderFilter {
    private IDbDialect dbDialect;

    private JdbcTemplate jdbcTemplate;

    private String tablePrefix;

    private String schemaPrefix;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        filter(context);
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        filter(context);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues) {
        filter(context);
        return true;
    }

    private void filter(IDataLoaderContext context) {
        if (!context.getTableName().startsWith(tablePrefix)
                && !context.getNodeId().equals(context.getTableTemplate().getTable().getSchema())) {
            ((DataLoaderContext) context).setTableTemplate(getTableTemplate(context));
        }
    }

    private TableTemplate getTableTemplate(IDataLoaderContext context) {
        TableTemplate tableTemplate = new TableTemplate(jdbcTemplate, dbDialect, context.getTableName(), null, false,
                schemaPrefix == null ? context.getNodeId() : schemaPrefix + context.getNodeId(), null);
        tableTemplate.setColumnNames(context.getColumnNames());
        tableTemplate.setKeyNames(context.getKeyNames());
        tableTemplate.setOldData(context.getOldData());
        return tableTemplate;
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setSchemaPrefix(String schemaPrefix) {
        this.schemaPrefix = schemaPrefix;
    }

}
