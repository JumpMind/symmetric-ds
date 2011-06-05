package org.jumpmind.symmetric.core;

import java.util.List;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.core.model.Table;

public class SymmetricClient {

    final static Log log = LogFactory.getLog(SymmetricClient.class);

    protected IEnvironment environment;

    public SymmetricClient(IEnvironment environment) {
        this.environment = environment;
        initServices();
    }

    public void initialize() {
        initDatabase();
    }

    public void syncTriggers() {
    }

    public RemoteNodeStatuses push() {
        return null;
    }

    public RemoteNodeStatuses pull() {
        return null;
    }

    protected void initServices() {
    }

    protected void initDatabase() {
        List<Table> tables = environment.getDbDialect().findTables(null, null, false);
        for (Table table : tables) {
            log.info(table.toVerboseString());
        }
    }

}
