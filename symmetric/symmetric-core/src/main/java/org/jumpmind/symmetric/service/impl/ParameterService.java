/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.symmetric.service.impl;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.ITypedPropertiesFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.model.DatabaseParameter;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.util.FormatUtils;

/**
 * @see IParameterService
 */
public class ParameterService implements IParameterService {

    protected Log log = LogFactory.getLog(getClass());

    private TypedProperties parameters;

    private long cacheTimeoutInMs = 0;

    private Date lastTimeParameterWereCached;

    private IParameterFilter parameterFilter;

    private Properties systemProperties;

    private boolean initialized = false;
    
    private String tablePrefix;

    private ITypedPropertiesFactory factory;

    private ParameterServiceSqlMap sql;

    private ISqlTemplate jdbcTemplate;

    public ParameterService(IDatabasePlatform platform, ITypedPropertiesFactory factory,
            String tablePrefix, Log log) {
        this.log = log;
        this.systemProperties = (Properties) System.getProperties().clone();
        this.tablePrefix = tablePrefix;
        this.factory = factory;
        this.sql = new ParameterServiceSqlMap(null,
                AbstractService.createSqlReplacementTokens(tablePrefix));
        this.jdbcTemplate = platform.getSqlTemplate();

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
        if (val != null) {
            return Integer.parseInt(val);
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

    public String getString(String key) {
        return getString(key, null);
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

    /**
     * Save a parameter that applies to {@link ParameterConstants#ALL} external
     * ids and all node groups.
     */
    public void saveParameter(String key, Object paramValue) {
        this.saveParameter(ParameterConstants.ALL, ParameterConstants.ALL, key, paramValue);
    }

    public void saveParameter(String externalId, String nodeGroupId, String key, Object paramValue) {
        paramValue = paramValue != null ? paramValue.toString() : null;

        int count = jdbcTemplate.update(sql.getSql("updateParameterSql"), new Object[] {
                paramValue, externalId, nodeGroupId, key });

        if (count == 0) {
            jdbcTemplate.update(sql.getSql("insertParameterSql"), new Object[] { externalId,
                    nodeGroupId, key, paramValue });
        }

        rereadParameters();
    }

    public void deleteParameter(String externalId, String nodeGroupId, String key) {
        jdbcTemplate.update(sql.getSql("deleteParameterSql"), externalId, nodeGroupId, key);
        rereadParameters();

    }

    public void saveParameters(String externalId, String nodeGroupId, Map<String, Object> parameters) {
        Set<String> keys = parameters.keySet();
        for (String key : keys) {
            saveParameter(externalId, nodeGroupId, key, parameters.get(key));
        }
    }

    public synchronized void rereadParameters() {
        this.parameters = null;
        getParameters();
    }

    private TypedProperties rereadDatabaseParameters(Properties p) {
        try {
            TypedProperties properties = rereadDatabaseParameters(ParameterConstants.ALL,
                    ParameterConstants.ALL);
            properties.putAll(rereadDatabaseParameters(ParameterConstants.ALL,
                    p.getProperty(ParameterConstants.NODE_GROUP_ID)));
            properties.putAll(rereadDatabaseParameters(
                    p.getProperty(ParameterConstants.EXTERNAL_ID),
                    p.getProperty(ParameterConstants.NODE_GROUP_ID)));
            return properties;
        } catch (Exception ex) {
            if (initialized) {
                log.warn("Could not read database parameters.  We will try again later");
            }
            return new TypedProperties();
        }
    }

    private TypedProperties rereadDatabaseParameters(String externalId, String nodeGroupId) {
        final TypedProperties properties = new TypedProperties();
        jdbcTemplate.query(sql.getSql("selectParametersSql"), new ISqlRowMapper<Object>() {
            public Object mapRow(Row row) {
                properties.setProperty(row.getString("param_key"), row.getString("param_value"));
                return null;
            }
        }, externalId, nodeGroupId);
        return properties;
    }

    private TypedProperties rereadApplicationParameters() {
        TypedProperties p = this.factory.reload();
        p.putAll(systemProperties);
        p.putAll(rereadDatabaseParameters(p));
        initialized = true;
        return p;
    }

    private TypedProperties getParameters() {
        if (parameters == null
                || lastTimeParameterWereCached == null
                || (cacheTimeoutInMs > 0 && lastTimeParameterWereCached.getTime() < (System
                        .currentTimeMillis() - cacheTimeoutInMs))) {
            lastTimeParameterWereCached = new Date();
            parameters = rereadApplicationParameters();
            cacheTimeoutInMs = getInt(ParameterConstants.PARAMETER_REFRESH_PERIOD_IN_MS);
        }
        return parameters;
    }

    public TypedProperties getAllParameters() {
        return getParameters();
    }

    public Date getLastTimeParameterWereCached() {
        return lastTimeParameterWereCached;
    }

    public void setParameterFilter(IParameterFilter parameterFilter) {
        this.parameterFilter = parameterFilter;
    }

    public String getExternalId() {
        return getWithHostName(ParameterConstants.EXTERNAL_ID);
    }

    public String getSyncUrl() {
        return getWithHostName(ParameterConstants.SYNC_URL);
    }

    public List<DatabaseParameter> getDatabaseParametersFor(String paramKey) {
        return jdbcTemplate.query(sql.getSql("selectParametersByKeySql"),
                new DatabaseParameterMapper(), paramKey);
    }

    public TypedProperties getDatabaseParametersByNodeGroupId(String nodeGroupId) {
        return rereadDatabaseParameters(ParameterConstants.ALL, nodeGroupId);
    }

    protected String getWithHostName(String paramKey) {
        String value = getString(paramKey);
        if (!StringUtils.isBlank(value)) {
            if (value.contains("hostName")) {
                value = FormatUtils.replace("hostName", AppUtils.getHostName(), value);
            }
            if (value.contains("ipAddress")) {
                value = FormatUtils.replace("ipAddress", AppUtils.getIpAddress(), value);
            }
        }
        return value;
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

    class DatabaseParameterMapper implements ISqlRowMapper<DatabaseParameter> {
        public DatabaseParameter mapRow(Row row) {
            return new DatabaseParameter(row.getString("param_key"), row.getString("param_value"),
                    row.getString("external_id"), row.getString("node_group_id"));
        }
    }

    public String getTablePrefix() {
        return this.tablePrefix;
    }
    
    public String getEngineName() {
        return getString(ParameterConstants.ENGINE_NAME, "SymmetricDS");
    }

}