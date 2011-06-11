package org.jumpmind.symmetric.core.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.db.SqlConstants;
import org.jumpmind.symmetric.core.ext.IParameterFilter;

public class Parameters extends HashMap<String, String> {

    private static final long serialVersionUID = 1L;

    protected static Log log = LogFactory.getLog(Parameters.class);

    public final static String PARAMETER_REFRESH_PERIOD_IN_MS = "parameter.reload.timeout.ms";

    public final static String DB_TABLE_PREFIX = "sync.table.prefix";

    public final static String DB_METADATA_IGNORE_CASE = "db.metadata.ignore.case";

    public final static String DB_USE_ALL_COLUMNS_AS_PK_IF_NONE_FOUND = "db.pk.use.all.if.none";

    public final static String DB_USE_PKS_FROM_SOURCE = "db.pk.use.from.source";

    public final static String DB_SUPPORT_BIG_LOBS = "db.support.big.lobs";

    public final static String DB_QUERY_TIMEOUT = "db.sql.query.timeout.seconds";

    public final static String DB_STREAMING_FETCH_SIZE = "db.jdbc.streaming.results.fetch.size";

    public final static String LOADER_CREATE_TABLE_IF_DOESNT_EXIST = "dataloader.create.if.table.doesnt.exist";

    public final static String LOADER_MAX_ROWS_BEFORE_COMMIT = "dataloader.max.rows.before.commit";

    public final static String LOADER_MAX_ROWS_BEFORE_BATCH_FLUSH = "dataloader.max.rows.before.batch.flush";

    public final static String LOADER_USE_BATCHING = "dataloader.use.batching";

    public final static String LOADER_ENABLE_FALLBACK_UPDATE = "dataloader.enable.fallback.update";

    public final static String LOADER_ENABLE_FALLBACK_INSERT = "dataloader.enable.fallback.insert";

    public final static String LOADER_ALLOW_MISSING_DELETES = "dataloader.allow.missing.delete";

    public final static String LOADER_DATA_FILTERS = "dataloader.filters";

    public final static String LOADER_DONT_INCLUDE_PKS_IN_UPDATE = "dataloader.dont.include.keys.in.update.statement";

    public final static String TRIGGER_NUMBER_PRECISION = "trigger.number.precision";
    
    public final static String EXTERNAL_ID = "external.id";
    
    public final static String NODE_GROUP_ID = "group.id";

    protected List<IParameterFilter> parameterFilters = new ArrayList<IParameterFilter>();

    public Parameters() {
    }

    public Parameters(List<IParameterFilter> parameterFilters) {
        this.parameterFilters.addAll(parameterFilters);
    }

    public Parameters(Properties properties) {
        putAll(properties);
    }

    public void putAll(Properties properties) {
        for (Object key : properties.keySet()) {
            put((String) key, properties.getProperty((String) key));
        }
    }

    public long getLong(String key, long defaultValue) {
        long returnValue = defaultValue;
        String value = get(key);
        if (value != null) {
            try {
                returnValue = Long.parseLong(value);
            } catch (NumberFormatException ex) {
                // TODO log error
            }
        }
        return returnValue;
    }

    public int getInt(String key, int defaultValue) {
        int returnValue = defaultValue;
        String value = get(key);
        if (value != null) {
            try {
                returnValue = Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                // TODO log error
            }
        }
        return returnValue;
    }

    public boolean is(String key, boolean defaultValue) {
        boolean returnValue = defaultValue;
        String value = get(key);
        if (value != null) {
            returnValue = Boolean.parseBoolean(value);
        }
        return returnValue;
    }

    public String get(String key, String defaultValue) {
        String returnValue = defaultValue;
        String value = get(key);
        if (value != null) {
            returnValue = value;
        }
        return returnValue;
    }     

    public String[] getArray(String key, String[] defaultValue) {
        String value = get(key);
        String[] retValue = defaultValue;
        if (value != null) {
            retValue = value.split(",");
        }
        return retValue;
    }   

    @SuppressWarnings("unchecked")
    public <T> List<T> instantiate(String key) {
        String[] clazzes = getArray(key, new String[0]);
        List<T> objects = new ArrayList<T>(clazzes.length);
        try {
            for (String clazz : clazzes) {
                Class<?> c = Class.forName(clazz);
                if (c != null) {
                    objects.add((T) c.newInstance());
                }
            }
            return objects;
        } catch (Exception ex) {
            log.log(LogLevel.WARN, ex);
            return objects;
        }
    }
    
    public String getExternalId() {
        return get(EXTERNAL_ID, "");
    }
    
    public String getNodeGroupId() {
        return get(NODE_GROUP_ID, "");
    }

    public int getQueryTimeout() {
        return getInt(DB_QUERY_TIMEOUT, SqlConstants.DEFAULT_QUERY_TIMEOUT_SECONDS);
    }

    public int getStreamingFetchSize() {
        return getInt(DB_STREAMING_FETCH_SIZE, SqlConstants.DEFAULT_STREAMING_FETCH_SIZE);
    }

    @Override
    public String get(Object key) {
        String value = super.get(key);
        for (IParameterFilter filter : parameterFilters) {
            value = filter.filterParameter((String)key, value);
        }
        return value;
    }

}
