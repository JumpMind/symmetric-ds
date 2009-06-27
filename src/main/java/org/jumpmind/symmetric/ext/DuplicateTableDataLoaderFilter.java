/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
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
package org.jumpmind.symmetric.ext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.TableTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 */
public class DuplicateTableDataLoaderFilter implements IDataLoaderFilter {
    private static final Log logger = LogFactory
            .getLog(DuplicateTableDataLoaderFilter.class);

    private IDbDialect dbDialect;

    private JdbcTemplate jdbcTemplate;

    private TableTemplate tableTemplate;
    
    private String duplicateSchema;
    
    private String duplicateCatalog;

    private String duplicateTableName;

    private String originalTableName;
    
    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        if (context.getTableName().equals(originalTableName)) {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.insert(context, keyValues);
        }
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context,
            String[] columnValues) {
        if (context.getTableName().equals(originalTableName)) {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.insert(context, columnValues);
        }
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context,
            String[] columnValues, String[] keyValues) {
        if (context.getTableName().equals(originalTableName)) {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.update(context, columnValues, keyValues);
        }
        return true;
    }

    private TableTemplate getTableTemplate(IDataLoaderContext context) {
        if (tableTemplate == null) {
            tableTemplate = new TableTemplate(jdbcTemplate, dbDialect,
                    duplicateTableName, null, false, duplicateSchema, duplicateCatalog);
            tableTemplate.setColumnNames(context.getColumnNames());
            tableTemplate.setKeyNames(context.getKeyNames());
        }
        return tableTemplate;
    }

    public boolean isAutoRegister() {
        logger.info("Duplicating table " + originalTableName + " into " + (duplicateCatalog != null ? duplicateCatalog + "." : "") + (duplicateSchema != null ? duplicateSchema + "." : "") +
                duplicateTableName);
        return true;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setOriginalTableName(String tableName) {
        this.originalTableName = tableName;
    }

    public void setDuplicateTableName(String duplicateTableName) {
        this.duplicateTableName = duplicateTableName;
    }

    public void setDuplicateSchema(String schema) {
        this.duplicateSchema = schema;
    }

    public void setDuplicateCatalog(String catalog) {
        this.duplicateCatalog = catalog;
    }
    
    

}
