package org.jumpmind.symmetric.db.db2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

public class ZSeriesDb2DbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(ZSeriesDb2DbDialect.class);

    // Reading Current schema properties
    private String currentSchema;
    private String userName;

    /**
     * Returns the database user id
     * @return String
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Sets the database user id from properties file
     * @param userName
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Returns the current schema name
     * @return String
     */
    public String getCurrentSchema() {
        return currentSchema;
    }

    /**
     * Sets the current schema name from properties file
     * @param currentSchema
     */
    public void setCurrentSchema(String currentSchema) {
        this.currentSchema = currentSchema;
    }

    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("SELECT COUNT(*) FROM SYSIBM.SYSTRIGGERS WHERE NAME = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public boolean isBlobSyncSupported() {
        return true;
    }

    public boolean isClobSyncSupported() {
        return true;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return "nullif('','')";
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
        return false;
    }

    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        if(getCurrentSchema() != null 
                && (!getCurrentSchema().trim().equals("")) 
                && (!getCurrentSchema().trim().equals("null")) ){
            return getCurrentSchema();
        }
        return getUserName().toUpperCase();
    }

    public String getIdentifierQuoteString() {
        return "";
    }

    public void disableSyncTriggers(String nodeId) {
    }

    public void enableSyncTriggers() {
    }

    public String getSyncTriggersExpression() {
        return "";
    }

    @Override
    protected void initForSpecificDialect() {
    }
}
