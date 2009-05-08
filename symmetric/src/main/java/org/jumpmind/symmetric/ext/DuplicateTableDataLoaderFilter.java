
package org.jumpmind.symmetric.ext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.TableTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author elong
 */
public class DuplicateTableDataLoaderFilter implements IDataLoaderFilter
{
    private static final Log logger = LogFactory.getLog(DuplicateTableDataLoaderFilter.class);

    private IDbDialect dbDialect;

    private JdbcTemplate jdbcTemplate;

    private TableTemplate tableTemplate;

    private String tableName;

    private String duplicateTableName;

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues)
    {
        if (context.getTableName().equals(tableName))
        {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.insert(context, keyValues);
        }
        return true;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues)
    {
        if (context.getTableName().equals(tableName))
        {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.insert(context, columnValues);
        }
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues)
    {
        if (context.getTableName().equals(tableName))
        {
            TableTemplate tableTemplate = getTableTemplate(context);
            tableTemplate.update(context, columnValues, keyValues);
        }
        return true;
    }

    private TableTemplate getTableTemplate(IDataLoaderContext context)
    {
        if (tableTemplate == null)
        {
            tableTemplate = new TableTemplate(jdbcTemplate, dbDialect, duplicateTableName, null, false);
            tableTemplate.setColumnNames(context.getColumnNames());
            tableTemplate.setKeyNames(context.getKeyNames());
        }
        return tableTemplate;
    }

    public boolean isAutoRegister()
    {
        logger.info("Duplicating table " + tableName + " into " + duplicateTableName);
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

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    public void setDuplicateTableName(String duplicateTableName)
    {
        this.duplicateTableName = duplicateTableName;
    }

}
