package org.jumpmind.symmetric.db;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IParameterService;

abstract public class AbstractEmbeddedSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {
    
    public AbstractEmbeddedSymmetricDialect(IParameterService parameterService,
            IDatabasePlatform platform) {
        super(parameterService, platform);
    }

    /**
     * All the templates have ' escaped because the SQL is inserted into a view.
     * When returning the raw SQL for use as SQL it needs to be un-escaped.
     */
    @Override
    public String createInitialLoadSqlFor(Node node, TriggerRouter trigger, Table table, TriggerHistory triggerHistory, Channel channel, String overrideSelectSql) {
        String sql = super.createInitialLoadSqlFor(node, trigger, table, triggerHistory, channel, overrideSelectSql);
        sql = sql.replace("''", "'");
        return sql;
    }

    @Override
    public String createCsvDataSql(Trigger trigger, TriggerHistory triggerHistory, Channel channel, String whereClause) {
        String sql = super.createCsvDataSql(trigger, triggerHistory, channel, whereClause);
        sql = sql.replace("''", "'");
        return sql;
    }

    @Override
    public String createCsvPrimaryKeySql(Trigger trigger, TriggerHistory triggerHistory, Channel channel, String whereClause) {
        String sql = super.createCsvPrimaryKeySql(trigger, triggerHistory, channel, whereClause);
        sql = sql.replace("''", "'");
        return sql;
    }

  
    public void purgeRecycleBin() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getInitialLoadTableAlias() {
        return "t.";
    }

    @Override
    public String preProcessTriggerSqlClause(String sqlClause) {
        sqlClause = sqlClause.replace("$(newTriggerValue).", "$(newTriggerValue)");
        sqlClause = sqlClause.replace("$(oldTriggerValue).", "$(oldTriggerValue)");
        sqlClause = sqlClause.replace("$(curTriggerValue).", "$(curTriggerValue)");
        return sqlClause.replace("'", "''");
    }
    
    @Override
    public boolean escapesTemplatesForDatabaseInserts() {
        return true;
    }
}