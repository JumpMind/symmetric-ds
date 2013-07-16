package org.jumpmind.db.platform.sqlite;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class SqliteDatabasePlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    /* The standard H2 driver. */
    public static final String JDBC_DRIVER = "org.sqlite.JDBC";
    
    private Map<String, String> sqlScriptReplacementTokens;
    
    public SqliteDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
        sqlScriptReplacementTokens = new HashMap<String, String>();
        sqlScriptReplacementTokens.put("current_timestamp", "strftime('%Y-%m-%d %H:%M:%f','now','localtime')");
        sqlScriptReplacementTokens.put("\\{ts([^<]*?)\\}","$1");
        sqlScriptReplacementTokens.put("\\{d([^<]*?)\\}","$1");
    }

    
    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
        return sqlScriptReplacementTokens;
    }
    
    public String getName() {
        return DatabaseNamesConstants.SQLITE;
    }

    public String getDefaultSchema() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getDefaultCatalog() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    protected SqliteDdlBuilder createDdlBuilder() {
        return  new SqliteDdlBuilder();
    }

    @Override
    protected SqliteDdlReader createDdlReader() {
        return new SqliteDdlReader(this);
    }    
    
    
    protected ISqlTemplate createSqlTemplate() {
        return new SqliteJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo()); 
    }
}
