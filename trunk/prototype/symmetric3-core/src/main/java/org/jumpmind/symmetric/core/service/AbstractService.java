package org.jumpmind.symmetric.core.service;

import java.util.List;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.Table;

abstract public class AbstractService {
    
    protected final Log log = LogFactory.getLog(getClass());

    protected IEnvironment environment;

    protected IDbDialect dbDialect;

    protected SymmetricTables tables;
    
    public AbstractService(IEnvironment environment) {
        this.environment = environment;
        this.dbDialect = environment.getDbDialect();
        this.tables = this.dbDialect.getSymmetricTables();
    }
    
    protected Table getTable(String tableSuffix) {
        return tables.getSymmetricTable(tableSuffix);
    }
    
    protected Table[] getTables(String... tableSuffixes) {
        return tables.getSymmetricTables(tableSuffixes);
    }
    
    protected <T> T getFirstEntry(List<T> list) {
        return list != null && list.size() > 0 ? list.get(0) : null;
    }
    
}
