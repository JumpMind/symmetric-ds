package org.jumpmind.symmetric.core.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.Query;
import org.jumpmind.symmetric.core.ext.IParameterFilter;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;

public class ParameterService extends AbstractService {

    private static final String ALL = "ALL";

    protected List<IParameterFilter> parameterFilters = new ArrayList<IParameterFilter>();

    private long cacheTimeoutInMs = 0;

    private Date lastTimeParameterWereCached;

    private Parameters parameterCache;

    public ParameterService(IEnvironment environment) {
        super(environment);
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

    /**
     * Save a parameter that applies to {@link ParameterConstants#ALL} external
     * ids and all node groups.
     */
    public void saveParameter(String key, Object paramValue) {
        this.saveParameter(ALL, ALL, key, paramValue);
    }

    public void saveParameter(String externalId, String nodeGroupId, String key, Object paramValue) {
        Table parameterTable = tables.getSymmetricTable(SymmetricTables.PARAMETER);
        dbDialect.getSqlTemplate().save(parameterTable,
                buildParams(externalId, nodeGroupId, key, paramValue));
        refresh();
    }

    public void deleteParameter(String externalId, String nodeGroupId, String key) {
        Table parameterTable = tables.getSymmetricTable(SymmetricTables.PARAMETER);
        dbDialect.getSqlTemplate().delete(parameterTable,
                buildParams(externalId, nodeGroupId, key, null));
        refresh();
    }

    public void saveParameters(String externalId, String nodeGroupId, Map<String, Object> parameters) {
        Set<String> keys = parameters.keySet();
        for (String key : keys) {
            saveParameter(externalId, nodeGroupId, key, parameters.get(key));
        }
    }
    
    protected Map<String, Object> buildParams(String externalId, String nodeGroupId, String key,
            Object paramValue) {
        Map<String, Object> params = new HashMap<String, Object>(4);
        params.put("EXTERNAL_ID", externalId);
        params.put("NODE_GROUP_ID", nodeGroupId);
        params.put("PARAM_KEY", key);
        params.put("PARAM_VALUE", paramValue != null ? paramValue.toString() : null);
        return params;
    }

    protected Parameters readParameters() {
        Parameters parameters = new Parameters(parameterFilters);
        parameters.putAll(System.getProperties());
        parameters.putAll(environment.getLocalParameters());
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
                .where("EXTERNAL_ID", "=", externalId).and("NODE_GROUP_ID", "=", groupId);
        ISqlTemplate template = this.dbDialect.getSqlTemplate();
        return template.query(query.getSql(), "PARAM_KEY", "PARAM_VALUE", query.getArgs(),
                query.getArgTypes());
    }

}
