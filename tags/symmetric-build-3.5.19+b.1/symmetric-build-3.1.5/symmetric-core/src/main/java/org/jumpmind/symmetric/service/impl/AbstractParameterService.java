package org.jumpmind.symmetric.service.impl;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractParameterService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected TypedProperties parameters;

    private long cacheTimeoutInMs = 0;

    private long lastTimeParameterWereCached;

    protected IParameterFilter parameterFilter;

    protected Properties systemProperties;

    private boolean databaseHasBeenInitialized = false;

    public AbstractParameterService() {
        this.systemProperties = (Properties) System.getProperties().clone();
    }

    public BigDecimal getDecimal(String key, BigDecimal defaultVal) {
        String val = getString(key);
        if (val != null) {
            return new BigDecimal(val);
        }
        return defaultVal;
    }

    public BigDecimal getDecimal(String key) {
        return getDecimal(key, BigDecimal.ZERO);
    }

    public boolean is(String key) {
        return is(key, false);
    }

    public boolean is(String key, boolean defaultVal) {
        String val = getString(key);
        if (val != null) {
            val = val.trim();
            if (val.equals("1")) {
                return true;
            } else {
                return Boolean.parseBoolean(val);
            }
        } else {
            return defaultVal;
        }
    }

    public int getInt(String key) {
        return getInt(key, 0);
    }

    public int getInt(String key, int defaultVal) {
        String val = getString(key);
        if (StringUtils.isNotBlank(val)) {
            return Integer.parseInt(val.trim());
        }
        return defaultVal;
    }

    public long getLong(String key) {
        return getLong(key, 0);
    }

    public long getLong(String key, long defaultVal) {
        String val = getString(key);
        if (val != null) {
            return Long.parseLong(val);
        }
        return defaultVal;
    }

    public String getString(String key, String defaultVal) {
        String value = null;

        if (StringUtils.isBlank(value)) {
            value = getParameters().get(key, defaultVal);
        }

        if (this.parameterFilter != null) {
            value = this.parameterFilter.filterParameter(key, value);
        }

        return StringUtils.isBlank(value) ? defaultVal : value;
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getTempDirectory() {
        return getString("java.io.tmpdir", System.getProperty("java.io.tmpdir"));
    }

    protected abstract TypedProperties rereadApplicationParameters();

    public synchronized void rereadParameters() {
        lastTimeParameterWereCached = 0;
        getParameters();
    }

    protected TypedProperties getParameters() {
        if (parameters == null
                || (cacheTimeoutInMs > 0 && lastTimeParameterWereCached < (System
                        .currentTimeMillis() - cacheTimeoutInMs))) {
            try {
                parameters = rereadApplicationParameters();
                lastTimeParameterWereCached = System.currentTimeMillis();
                cacheTimeoutInMs = getInt(ParameterConstants.PARAMETER_REFRESH_PERIOD_IN_MS);
            } catch (SqlException ex) {
                log.error("Could not read database parameters.  We will try again later");
            }
        }
        return parameters;
    }

    public TypedProperties getAllParameters() {
        return getParameters();
    }

    public Date getLastTimeParameterWereCached() {
        return new Date(lastTimeParameterWereCached);
    }

    public void setParameterFilter(IParameterFilter parameterFilter) {
        this.parameterFilter = parameterFilter;
    }

    public String getExternalId() {
        return substituteVariables(ParameterConstants.EXTERNAL_ID);
    }

    public String getSyncUrl() {
        return substituteVariables(ParameterConstants.SYNC_URL);
    }

    public String getNodeGroupId() {
        return getString(ParameterConstants.NODE_GROUP_ID);
    }

    public String getRegistrationUrl() {
        String url = getString(ParameterConstants.REGISTRATION_URL);
        if (url != null) {
            url = url.trim();
        }
        return url;
    }

    public Map<String, String> getReplacementValues() {
        Map<String, String> replacementValues = new HashMap<String, String>(2);
        replacementValues.put("externalId", getExternalId());
        replacementValues.put("nodeGroupId", getNodeGroupId());
        return replacementValues;
    }

    public String getEngineName() {
        return getString(ParameterConstants.ENGINE_NAME, "SymmetricDS");
    }

    public void setDatabaseHasBeenInitialized(boolean databaseHasBeenInitialized) {
        this.databaseHasBeenInitialized = databaseHasBeenInitialized;
    }

    abstract protected TypedProperties rereadDatabaseParameters(String externalId,
            String nodeGroupId);

    protected TypedProperties rereadDatabaseParameters(Properties p) {
        try {
            TypedProperties properties = rereadDatabaseParameters(ParameterConstants.ALL,
                    ParameterConstants.ALL);
            properties.putAll(rereadDatabaseParameters(ParameterConstants.ALL,
                    p.getProperty(ParameterConstants.NODE_GROUP_ID)));
            properties.putAll(rereadDatabaseParameters(
                    p.getProperty(ParameterConstants.EXTERNAL_ID),
                    p.getProperty(ParameterConstants.NODE_GROUP_ID)));
            databaseHasBeenInitialized = true;
            return properties;
        } catch (SqlException ex) {
            if (databaseHasBeenInitialized) {
                throw ex;
            } else {
                return new TypedProperties();
            }
        }
    }

    protected String substituteVariables(String paramKey) {
        String value = getString(paramKey);
        if (!StringUtils.isBlank(value)) {
            if (value.contains("hostName")) {
                value = FormatUtils.replace("hostName", AppUtils.getHostName(), value);
            }
            if (value.contains("ipAddress")) {
                value = FormatUtils.replace("ipAddress", AppUtils.getIpAddress(), value);
            }
            if (value.contains("engineName")) {
                value = FormatUtils.replace("engineName", getEngineName(), value);
            }
        }
        return value;
    }

}