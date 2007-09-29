package org.jumpmind.symmetric.db;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.Trigger;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

public interface IDbDialect {

    public void initTrigger(DataEventType dml, Trigger config,
            TriggerHistory audit, String tablePrefix, Table table);

    public void removeTrigger(String schemaName, String triggerName);

    public void initConfigDb(String tablePrefix);

    public Platform getPlatform();
    
    public String getName();
    
    public String getVersion();

    public boolean doesTriggerExist(String schema, String tableName, String triggerName);

    public Table getMetaDataFor(String schema, final String tableName, boolean useCache);

    public String getTransactionTriggerExpression();

    public String createInitalLoadSqlFor(Node client, Trigger config);

    public boolean isCharSpacePadded();
    
    public boolean isCharSpaceTrimmed();
    
    public boolean isEmptyStringNulled();
    
    public void purge();
    
    public SQLErrorCodeSQLExceptionTranslator getSqlErrorTranslator();
    
    public void disableSyncTriggers();

    public void enableSyncTriggers();
    
    public String getDefaultSchema();
    
}
