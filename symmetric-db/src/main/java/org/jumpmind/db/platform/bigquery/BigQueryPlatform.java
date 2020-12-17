package org.jumpmind.db.platform.bigquery;

import org.jumpmind.db.platform.AbstractDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

import com.google.cloud.bigquery.BigQuery;

public class BigQueryPlatform extends AbstractDatabasePlatform {

    ISqlTemplate sqlTemplate;
    BigQuery bigquery;
    
    public BigQueryPlatform(SqlTemplateSettings settings, BigQuery bigquery) {
        super(settings);
        
        this.bigquery = bigquery;
        sqlTemplate = new BigQuerySqlTemplate(bigquery);
        this.ddlBuilder = new BigQueryDdlBuilder(bigquery);
        this.ddlReader = new BigQueryDdlReader(bigquery);
    }

    @Override
    public String getName() {
        return "bigquery";
    }

    @Override
    public String getDefaultSchema() {
        return null;
    }

    @Override
    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public <T> T getDataSource() {
        return null;
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return sqlTemplate;
    }

    @Override
    public ISqlTemplate getSqlTemplateDirty() {
        return null;
    }
    
    @Override
    public IDdlBuilder getDdlBuilder() {
        return this.ddlBuilder;
    }
    
    @Override
    public IDdlReader getDdlReader() {
        return new BigQueryDdlReader(this.bigquery);
    }

    public BigQuery getBigQuery() {
        return bigquery;
    }
    
    @Override
    public boolean supportsLimitOffset() {
        return true;
    }
    
    @Override
    public String massageForLimitOffset(String sql, int limit, int offset) {
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql + " limit " + limit + " offset " + offset + ";";
    }

}
