package org.jumpmind.symmetric.db.sqlite;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

public class SqliteSymmetricDialect extends AbstractSymmetricDialect {

    public SqliteSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new SqliteTriggerTemplate(this);
    }
    
    @Override
    protected void createRequiredFunctions() {
    }
    
    @Override
    protected void dropRequiredFunctions() {
    }

    public void purge() {
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
    }

    public String getSyncTriggersExpression() {
        return "1=1";
    }

    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return platform.getSqlTemplate().queryForInt(
                "select count(*) from sqlite_master where type='trigger' and name=? and tbl_name=?", triggerName,
                tableName) > 0;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }
    
    //TODO  Blobs and Clobs could be supported...
    
    public boolean isBlobSyncSupported() {
        return false;
    }

    //TODO
    public boolean isClobSyncSupported() {
        return false;
    }
    
}
