package org.jumpmind.symmetric.db.db2;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

public class Db2DbDialect extends AbstractDbDialect implements IDbDialect {

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "db2admin.sync_triggers_disabled";
    
    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "db2admin.sync_node_disabled";
	
    static final Log logger = LogFactory.getLog(Db2DbDialect.class);
        
    private Map<String, String> envVariablesToInstall;
    
    private String envVariableInstalledSql;

    protected void initForSpecificDialect() {
    	createRequiredEnvVariables();
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        return true;
    }

    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("select count(*) from syscat.triggers where trigname = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger " + triggerName + " does not exist");
        }
    }

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName) {
        removeTrigger(schemaName, triggerName);
    }

    public boolean isBlobSyncSupported() {
        // TODO:
        return false;
    }

    public boolean isClobSyncSupported() {
        return true;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public void disableSyncTriggers(String nodeId) {
    	jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
        if (nodeId != null) {
            jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
        }
    }

    public void enableSyncTriggers() {
    	jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=0");
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='N'");
    }

    public String getSyncTriggersExpression() {
        // TODO:
        //return "fn_sym_sync_triggers_disabled() = 0";
        return "1 = 1";
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return "fn_sym_transaction_id()";
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        return "values IDENTITY_VAL_LOCAL()";
    }

    public boolean isCharSpacePadded() {
        return true;
    }

    public boolean isCharSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    public boolean supportsGetGeneratedKeys() {
        // TODO:
        return false;
    }

    protected boolean allowsNullForIdentityColumn() {
        // TODO:
        return false;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("values CURRENT SCHEMA", String.class);
    }
    
    public String getIdentifierQuoteString()
    {
        return "";
    }



	public void setEnvVariablesToInstall(Map<String, String> envVariablesToInstall) {
		this.envVariablesToInstall = envVariablesToInstall;
	}

    public String[] getEnvVariablesToInstall() {
        if (envVariablesToInstall != null) {
            return envVariablesToInstall.keySet().toArray(new String[envVariablesToInstall.size()]);
        } else {
            return new String[0];
        }
    }

    public String createEnvVariablesDDL(String name) {
        if (envVariablesToInstall != null) {
            return envVariablesToInstall.get(name);
        } else {
            return null;
        }
    }
    
    public String getEnvVariablesSql(String variableName) {
        if (this.envVariablesToInstall != null) {
            return this.envVariablesToInstall.get(variableName);
        } else {
            return null;
        }
    }

    public String getEnvVariablesInstalledSql(String variableName) {
        if (envVariablesToInstall != null) {
            String ddl = replace("variableName", variableName, envVariableInstalledSql);
            return ddl;
        } else {
            return null;
        }
    }    
    
    private String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }
    
    protected void createRequiredEnvVariables() {
        String[] variables = this.getEnvVariablesToInstall();
        for (String varName : variables) {
            if (jdbcTemplate.queryForInt(this.getEnvVariablesInstalledSql(varName)) == 0) {
                jdbcTemplate.update(sqlTemplate.getFunctionSql(varName));
                logger.info("Just installed " + varName);
            }
        }
    }



	public void setEnvVariableInstalledSql(String envVariableInstalledSql) {
		this.envVariableInstalledSql = envVariableInstalledSql;
	}    

}

