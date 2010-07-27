/**
 * Copyright (C) 2005 Big Lots Inc.
 */

package org.jumpmind.symmetric.ext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.TableTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author elong
 */
public class SchemaPerNodeDataLoaderFilter implements IDataLoaderFilter
{
    private static final Log logger = LogFactory.getLog(SchemaPerNodeDataLoaderFilter.class);

    private IDbDialect dbDialect;

    private JdbcTemplate jdbcTemplate;

    private String tablePrefix;

    private String schemaPrefix;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues)
    {
        filter(context);
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues)
    {
        filter(context);
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues)
    {
        filter(context);
        return true;
    }

    private void filter(IDataLoaderContext context)
    {
        if (!context.getNodeId().equals("00000") && !context.getTableName().startsWith(tablePrefix)
            && !context.getNodeId().equals(context.getTableTemplate().getTable().getSchema()))
        {
            ((DataLoaderContext) context).setTableTemplate(getTableTemplate(context));
        }
    }

    private TableTemplate getTableTemplate(IDataLoaderContext context)
    {
        TableTemplate tableTemplate = new TableTemplate(jdbcTemplate, dbDialect, context.getTableName(), null, false,
            schemaPrefix == null ? context.getNodeId() : schemaPrefix + context.getNodeId(), null);
        tableTemplate.setColumnNames(context.getColumnNames());
        tableTemplate.setKeyNames(context.getKeyNames());
        tableTemplate.setOldData(context.getOldData());
        return tableTemplate;
    }

    public boolean isAutoRegister()
    {
        logger.info("Registered data loader filter " + getClass().getSimpleName());
        return true;
    }

    public void setDbDialect(IDbDialect dbDialect)
    {
        this.dbDialect = dbDialect;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate)
    {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setTablePrefix(String tablePrefix)
    {
        this.tablePrefix = tablePrefix;
    }

    public void setSchemaPrefix(String schemaPrefix)
    {
        this.schemaPrefix = schemaPrefix;
    }

}
