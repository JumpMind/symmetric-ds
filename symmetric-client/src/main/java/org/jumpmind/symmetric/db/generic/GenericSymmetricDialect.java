package org.jumpmind.symmetric.db.generic;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

public class GenericSymmetricDialect extends AbstractSymmetricDialect {

    public GenericSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new GenericTriggerTemplate(this);
        this.supportsSubselectsInDelete = false;
        this.supportsSubselectsInUpdate = false;
    }

    @Override
    public void cleanDatabase() {
    }

    @Override
    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
    }

    @Override
    public void enableSyncTriggers(ISqlTransaction transaction) {
    }

    @Override
    public String getSyncTriggersExpression() {
        return null;
    }

    @Override
    public void dropRequiredDatabaseObjects() {
    }

    @Override
    public void createRequiredDatabaseObjects() {
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return null;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return false;
    }

}
