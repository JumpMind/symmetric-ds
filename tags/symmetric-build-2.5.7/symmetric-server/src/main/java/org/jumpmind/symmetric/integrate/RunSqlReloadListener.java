package org.jumpmind.symmetric.integrate;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.load.IReloadListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataService;

public class RunSqlReloadListener implements IReloadListener {

    private IDataService dataService;

    private String sqlToRunAtTargetBeforeReload;

    private String sqlToRunAtTargetAfterReload;

    public void afterReload(Node node) {
        if (StringUtils.isNotBlank(sqlToRunAtTargetAfterReload)) {
            dataService.insertSqlEvent(node, sqlToRunAtTargetAfterReload, true);
        }
    }

    public void beforeReload(Node node) {
        if (StringUtils.isNotBlank(sqlToRunAtTargetBeforeReload)) {
            dataService.insertSqlEvent(node, sqlToRunAtTargetBeforeReload, true);
        }
    }

    public boolean isAutoRegister() {
        return true;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setSqlToRunAtTargetAfterReload(String sqlToRunAfterReload) {
        this.sqlToRunAtTargetAfterReload = sqlToRunAfterReload;
    }

    public void setSqlToRunAtTargetBeforeReload(String sqlToRunBeforeReload) {
        this.sqlToRunAtTargetBeforeReload = sqlToRunBeforeReload;
    }

}
