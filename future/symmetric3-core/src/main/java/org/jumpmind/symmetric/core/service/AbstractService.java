package org.jumpmind.symmetric.core.service;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.db.IDbDialect;

abstract public class AbstractService {

    protected IEnvironment environment;

    protected IDbDialect dbDialect;

    protected SymmetricTables tables;
    
    public AbstractService(IEnvironment environment) {
        this.environment = environment;
        this.dbDialect = environment.getDbDialect();
        this.tables = this.dbDialect.getSymmetricTables();
    }
}
