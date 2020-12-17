package org.jumpmind.db.platform.hana;

import java.sql.Connection;

import javax.sql.DataSource;

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class HanaDatabasePlatform extends AbstractJdbcDatabasePlatform {

    public HanaDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    public ISqlTemplate getSqlTemplate() {
        return new HanaSqlJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }
    
    @Override
    public String getName() {
        return DatabaseNamesConstants.HANA;
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
    protected IDdlBuilder createDdlBuilder() {
        return new HanaDdlBuilder();
    }

    @Override
    protected IDdlReader createDdlReader() {
        return new HanaDdlReader(this);
    }
    
    protected ISqlTemplate createSqlTemplateDirty() {
        JdbcSqlTemplate template = (JdbcSqlTemplate) super.createSqlTemplateDirty();
        template.setIsolationLevel(Connection.TRANSACTION_READ_COMMITTED);
        return template;
    }

    @Override
    protected PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";
           
        String triggerSql = "CREATE TRIGGER TEST_TRIGGER AFTER UPDATE ON " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter + 
                " BEGIN DECLARE PERMISSION_COUNT INT; " + 
                "     SELECT COUNT(*) INTO PERMISSION_COUNT FROM SYM_PERMISSION_TEST; END";  
        
        PermissionResult result = new PermissionResult(PermissionType.CREATE_TRIGGER, triggerSql);
        
        try {
            getSqlTemplate().update(triggerSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE TRIGGER permission or TRIGGER permission");
        }
        
        return result;
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
