package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.RemoteNodeStatuses;

public class SymmetricClient {

    final static Log log = LogFactory.getLog(SymmetricClient.class);

    protected IEnvironment environment;

    protected IDbDialect dbDialect;

    protected SymmetricTables symmetricDatabase;

    public SymmetricClient(IEnvironment environment) {
        this.environment = environment;
        this.dbDialect = this.environment.getDbDialect();
        this.symmetricDatabase = this.dbDialect.getSymmetricTables();
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
        this.dbDialect.alter(true, this.symmetricDatabase.getTables());
    }

}
