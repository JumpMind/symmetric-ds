package org.jumpmind.symmetric.integrate;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.ext.ISymmetricEngineAware;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Node;

public class RunSqlReloadListener implements IReloadListener, ISymmetricEngineAware {

    private ISymmetricEngine engine;

    private String sqlToRunAtTargetBeforeReload;

    private String sqlToRunAtTargetAfterReload;

    public void afterReload(ISqlTransaction transaction, Node node) {
        if (StringUtils.isNotBlank(sqlToRunAtTargetAfterReload)) {
            engine.getDataService().insertSqlEvent(transaction, node, sqlToRunAtTargetAfterReload, true);
        }
    }

    public void beforeReload(ISqlTransaction transaction, Node node) {
        if (StringUtils.isNotBlank(sqlToRunAtTargetBeforeReload)) {
            engine.getDataService().insertSqlEvent(transaction, node, sqlToRunAtTargetBeforeReload, true);
        }
    }

    public void setSqlToRunAtTargetAfterReload(String sqlToRunAfterReload) {
        this.sqlToRunAtTargetAfterReload = sqlToRunAfterReload;
    }

    public void setSqlToRunAtTargetBeforeReload(String sqlToRunBeforeReload) {
        this.sqlToRunAtTargetBeforeReload = sqlToRunBeforeReload;
    }

    public void setSymmetricEngine(ISymmetricEngine engine) {
       this.engine=engine;        
    }

}
