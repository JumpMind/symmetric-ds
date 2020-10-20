package org.jumpmind.db.platform.tibero;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

public class TiberoDatabasePlatform extends AbstractJdbcDatabasePlatform {

    public static final String JDBC_DRIVER = "com.tmax.tibero.jdbc.TbDriver";

    public static final String JDBC_SUBPROTOCOL_THIN = "tibero:thin";

    /*
     * Creates a new platform instance.
     */
    public TiberoDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    protected TiberoDdlBuilder createDdlBuilder() {
        return new TiberoDdlBuilder();
    }

    @Override
    protected TiberoDdlReader createDdlReader() {
        return new TiberoDdlReader(this);
    }

    @Override
    protected TiberoJdbcSqlTemplate createSqlTemplate() {
        return new TiberoJdbcSqlTemplate(dataSource, settings, new TiberoLobHandler(), getDatabaseInfo());
    }

    @Override
    protected ISqlTemplate createSqlTemplateDirty() {
        return sqlTemplate;
    }

    public String getName() {
        return DatabaseNamesConstants.TIBERO;
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject(
                    "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual", String.class);
        }
        return defaultSchema;
    }

    @Override
    public boolean canColumnBeUsedInWhereClause(Column column) {
        String jdbcTypeName = column.getJdbcTypeName();
        return !column.isOfBinaryType() || "RAW".equals(jdbcTypeName);
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";
           
        String triggerSql = "CREATE OR REPLACE TRIGGER TEST_TRIGGER AFTER UPDATE ON " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter + " BEGIN END";  
        
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
    public PermissionResult getExecuteSymPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";
           
        String executeSql = "SELECT DBMS_LOB.GETLENGTH('TEST'), UTL_RAW.CAST_TO_RAW('TEST') FROM DUAL";  
        
        PermissionResult result = new PermissionResult(PermissionType.EXECUTE, executeSql);
        
        try {
            getSqlTemplate().update(executeSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant EXECUTE on DBMS_LOB and UTL_RAW");
        }
        
        return result;
    }

    @Override
    public long getEstimatedRowCount(Table table) {
        return getSqlTemplateDirty().queryForLong("select nvl(num_rows,-1) from all_tables where table_name = ? and owner = ?",
                table.getName(), table.getSchema());
    }

}
