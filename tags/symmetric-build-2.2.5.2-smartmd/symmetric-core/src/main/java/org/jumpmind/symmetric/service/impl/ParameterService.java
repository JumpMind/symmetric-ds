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
 * under the License.  */
package org.jumpmind.symmetric.service.impl;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.IParameterFilter;
import org.jumpmind.symmetric.model.DatabaseParameter;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.jdbc.core.RowMapper;

/**
 * @see IParameterService
 */
public class ParameterService extends AbstractService implements IParameterService,
        BeanFactoryAware {

    private Map<String, String> parameters;

    private BeanFactory beanFactory;

    private long cacheTimeoutInMs = 0;

    private Date lastTimeParameterWereCached;

    private IParameterFilter parameterFilter;

    private Properties systemProperties;

    private boolean initialized = false;

    public ParameterService() {
        systemProperties = (Properties) System.getProperties().clone();
    }

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
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
            value = getParameters().get(key);
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

        int count = jdbcTemplate.update(getSql("updateParameterSql"), new Object[] { paramValue,
                externalId, nodeGroupId, key });

        if (count == 0) {
            jdbcTemplate.update(getSql("insertParameterSql"), new Object[] { externalId,
                    nodeGroupId, key, paramValue });
        }

        rereadParameters();
    }
    
    public void deleteParameter(String externalId, String nodeGroupId, String key) {
        jdbcTemplate.update(getSql("deleteParameterSql"), externalId, nodeGroupId, key);
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

    /**
     * Every time we pull the properties out of the bean factory they should get
     * reread from the file system.
     */
    private Properties rereadFileParameters() {
        return (Properties) beanFactory.getBean(Constants.PROPERTIES);
    }

    private Map<String, String> rereadDatabaseParameters(Properties p) {
        try {
            Map<String, String> map = rereadDatabaseParameters(ParameterConstants.ALL, ParameterConstants.ALL);
            map.putAll(rereadDatabaseParameters(ParameterConstants.ALL, p
                    .getProperty(ParameterConstants.NODE_GROUP_ID)));
            map.putAll(rereadDatabaseParameters(p.getProperty(ParameterConstants.EXTERNAL_ID), p
                    .getProperty(ParameterConstants.NODE_GROUP_ID)));
            return map;
        } catch (Exception ex) {
            if (initialized) {
                log.warn("DatabaseParametersReadingFailed");
            }
            return new HashMap<String, String>();
        }
    }

    private Map<String, String> rereadDatabaseParameters(String externalId, String nodeGroupId) {
        final Map<String, String> map = new HashMap<String, String>();
        jdbcTemplate.query(getSql("selectParametersSql"), new Object[] { externalId, nodeGroupId },
                new RowMapper<Object>() {
                    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                        map.put(rs.getString(1), rs.getString(2));
                        return null;
                    }
                });
        return map;
    }

    private Map<String, String> rereadApplicationParameters() {
        final Map<String, String> map = new HashMap<String, String>();
        Properties p = rereadFileParameters();
        p.putAll(systemProperties);
        for (Object key : p.keySet()) {
            map.put((String) key, p.getProperty((String) key));
        }
        map.putAll(rereadDatabaseParameters(p));
        initialized = true;
        return map;

    }

    private Map<String, String> getParameters() {
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

    public Map<String, String> getAllParameters() {
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
        String value = getWithHostName(ParameterConstants.SYNC_URL);
        if (StringUtils.isBlank(value)) {
            value = getWithHostName("my.url");
            if (!StringUtils.isBlank(value)) {
                log.warn("DeprecatedPropertyMsg", "my.url", ParameterConstants.SYNC_URL);
            }
        }
        return value;
    }
    
    public List<DatabaseParameter> getDatabaseParametersFor(String paramKey) {
        return jdbcTemplate.query(getSql("selectParametersByKeySql"), new DatabaseParameterMapper(), paramKey);
    }
    
    public Map<String,String> getDatabaseParametersByNodeGroupId(String nodeGroupId) {
        return rereadDatabaseParameters(ParameterConstants.ALL, nodeGroupId);
    }

    protected String getWithHostName(String paramKey) {
        String value = getString(paramKey);
        if (!StringUtils.isBlank(value)) {
            if (value.contains("hostName")) {
                value = AppUtils.replace("hostName", AppUtils.getHostName(), value);
            }
            if (value.contains("ipAddress")) {
                value = AppUtils.replace("ipAddress", AppUtils.getIpAddress(), value);
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
        Map<String,String> replacementValues = new HashMap<String, String>(2);
        replacementValues.put("externalId", getExternalId());
        replacementValues.put("nodeGroupId", getNodeGroupId());
        return replacementValues;
    }
    
    class DatabaseParameterMapper implements RowMapper<DatabaseParameter> {
        public DatabaseParameter mapRow(ResultSet rs, int rowNum) throws SQLException {            
            return new DatabaseParameter(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4));
        }
    }

}