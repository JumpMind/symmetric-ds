/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service.impl;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.jdbc.core.RowMapper;

public class ParameterService extends AbstractService implements IParameterService, BeanFactoryAware {

    static final Log logger = LogFactory.getLog(ParameterService.class);

    private Map<String, String> parameters;

    private BeanFactory beanFactory;

    private long cacheTimeoutInMs = 0;

    private Date lastTimeParameterWereCached;

    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }

    public BigDecimal getDecimal(String key) {
        String val = getString(key);
        if (val != null) {
            return new BigDecimal(val);
        }
        return BigDecimal.ZERO;
    }

    public boolean is(String key) {
        String val = getString(key);
        if (val != null) {
            if (val.equals("1")) {
                return true;
            } else {
                return Boolean.parseBoolean(val);
            }
        } else {
            return false;
        }
    }

    public int getInt(String key) {
        String val = getString(key);
        if (val != null) {
            return Integer.parseInt(val);
        }
        return 0;
    }

    public long getLong(String key) {
        String val = getString(key);
        if (val != null) {
            return Long.parseLong(val);
        }
        return 0;
    }

    public String getString(String key) {
        return getParameters().get(key);
    }

    public void saveParameter(String key, Object paramValue) {
        this.saveParameter(runtimeConfiguration.getExternalId(), runtimeConfiguration.getNodeGroupId(), key,
                paramValue);
    }

    public void saveParameter(String externalId, String nodeGroupId, String key, Object paramValue) {
        int count = jdbcTemplate.update(getSql("updateParameterSql"), new Object[] { paramValue, externalId,
                nodeGroupId, key });

        if (count == 0) {
            jdbcTemplate.update(getSql("insertParameterSql"), new Object[] { externalId, nodeGroupId, key,
                    paramValue });
        }

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
     * Every time we pull the properties out of the bean factory they should get reread from the file system.
     */
    private Properties rereadFileParameters() {
        return (Properties) beanFactory.getBean(Constants.PROPERTIES);
    }

    private Map<String, String> rereadDatabaseParameters() {
        Map<String, String> map = rereadDatabaseParameters(ALL, ALL);
        map.putAll(rereadDatabaseParameters(ALL, runtimeConfiguration.getNodeGroupId()));
        map.putAll(rereadDatabaseParameters(runtimeConfiguration.getExternalId(), runtimeConfiguration
                .getNodeGroupId()));
        return map;
    }

    private Map<String, String> rereadDatabaseParameters(String externalId, String nodeGroupId) {
        final Map<String, String> map = new HashMap<String, String>();
        jdbcTemplate.query(getSql("selectParametersSql"), new Object[] { externalId, nodeGroupId },
                new RowMapper() {
                    public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
                        map.put(rs.getString(1), rs.getString(2));
                        return null;
                    }
                });
        return map;
    }

    private Map<String, String> buildSystemParameters() {
        final Map<String, String> map = new HashMap<String, String>();
        Properties p = rereadFileParameters();
        for (Object key : p.keySet()) {
            map.put((String) key, p.getProperty((String) key));
        }
        map.putAll(rereadDatabaseParameters());
        return map;

    }

    private Map<String, String> getParameters() {
        if (parameters == null
                || lastTimeParameterWereCached == null
                || (cacheTimeoutInMs > 0 && lastTimeParameterWereCached.getTime() < (System
                        .currentTimeMillis() - cacheTimeoutInMs))) {
            lastTimeParameterWereCached = new Date();
            parameters = buildSystemParameters();
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

}
