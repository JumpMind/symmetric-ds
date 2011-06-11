package org.jumpmind.symmetric.core.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.Query;
import org.jumpmind.symmetric.core.ext.IParameterFilter;
import org.jumpmind.symmetric.core.model.Parameters;

public class ParameterService {

    private static final String ALL = "ALL";

    protected IEnvironment environment;

    protected IDbDialect dbDialect;

    protected SymmetricTables tables;

    protected List<IParameterFilter> parameterFilters = new ArrayList<IParameterFilter>();

    private long cacheTimeoutInMs = 0;

    private Date lastTimeParameterWereCached;

    private Parameters parameterCache;

    public ParameterService(IEnvironment environment) {
        this.environment = environment;
        this.dbDialect = environment.getDbDialect();
        this.tables = this.dbDialect.getSymmetricTables();
    }

    public void addParameterFilter(IParameterFilter parameterFilter) {
        this.parameterFilters.add(parameterFilter);
    }

    public boolean removeParameterFilter(IParameterFilter parameterFilter) {
        return this.parameterFilters.remove(parameterFilter);
    }

    public synchronized void refresh() {
        this.parameterCache = null;
        getParameters();
    }

    public Parameters getParameters() {
        if (this.parameterCache == null
                || this.lastTimeParameterWereCached == null
                || (this.cacheTimeoutInMs > 0 && this.lastTimeParameterWereCached.getTime() < (System
                        .currentTimeMillis() - this.cacheTimeoutInMs))) {
            this.lastTimeParameterWereCached = new Date();
            this.parameterCache = readParameters();
            this.dbDialect.refreshParameters(parameterCache);
            this.cacheTimeoutInMs = parameterCache.getInt(
                    Parameters.PARAMETER_REFRESH_PERIOD_IN_MS, 60000);
        }
        return this.parameterCache;
    }

    protected Parameters readParameters() {
        Parameters parameters = new Parameters(parameterFilters);
        parameters.putAll(System.getProperties());
        parameters.putAll(environment.getParameters());
        String externalId = parameters.getExternalId();
        String nodeGroupId = parameters.getNodeGroupId();
        parameters.putAll(readFromDatabase(ALL, ALL));
        parameters.putAll(readFromDatabase(nodeGroupId, ALL));
        parameters.putAll(readFromDatabase(nodeGroupId, externalId));
        return parameters;
    }

    protected Map<String, String> readFromDatabase(String groupId, String externalId) {
        Query query = this.dbDialect
                .createQuery(tables.getSymmetricTable(SymmetricTables.PARAMETER))
                .where("external_id", "=", externalId).and("node_group_id", "=", groupId);
        ISqlTemplate template = this.dbDialect.getSqlTemplate();
        return template.query(query.getSql(), "param_key", "param_value", query.getArgs(),
                query.getArgTypes());
    }

}
