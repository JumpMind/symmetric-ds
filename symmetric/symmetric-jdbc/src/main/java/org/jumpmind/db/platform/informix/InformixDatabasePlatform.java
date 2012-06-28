package org.jumpmind.db.platform.informix;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class InformixDatabasePlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    public static final String JDBC_DRIVER = "com.informix.jdbc.IfxDriver";

    public static final String JDBC_SUBPROTOCOL = "informix-sqli";
    
    private Map<String, String> sqlScriptReplacementTokens;

    public InformixDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);

        ddlReader = new InformixDdlReader(this);
        ddlBuilder = new InformixDdlBuilder();
        
        sqlScriptReplacementTokens = new HashMap<String, String>();
        sqlScriptReplacementTokens.put("current_timestamp", "current");
    }
    
    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new InformixJdbcSqlTemplate(dataSource, settings, null);
    }

    public String getName() {
        return DatabaseNamesConstants.INFORMIX;
    }
    

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = getSqlTemplate().queryForObject("select trim(user) from sysmaster:sysdual",
                    String.class);
        }
        return defaultSchema;
    }
        
    @Override
    public Map<String, String> getSqlScriptReplacementTokens() {
        return sqlScriptReplacementTokens;
    }
    
    @Override
    public boolean isClob(int type) {
        return type == Types.CLOB;
    }
    
}
