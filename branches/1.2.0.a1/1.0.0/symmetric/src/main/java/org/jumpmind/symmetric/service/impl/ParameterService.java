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

import static org.jumpmind.symmetric.common.Constants.GLOBAL_CONFIGURATION_ID;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.GlobalParameter;
import org.jumpmind.symmetric.model.GlobalParameterType;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.dao.DataIntegrityViolationException;

public class ParameterService extends AbstractService implements
        IParameterService {

    static final Log logger = LogFactory.getLog(ParameterService.class);

    static final String TABLE = "global_parameter";
    
    private String tablePrefix;

    Map<String, Object> defaultGlobalParameters;

    public BigDecimal getDecimal(String configurationId, GlobalParameter key) {
        return null;
    }

    public int getInt(String configurationId, GlobalParameter key) {
        return 0;
    }

    public long getLong(String configurationId, GlobalParameter key) {
        return 0;
    }

    public String getString(String configurationId, GlobalParameter key) {
        return null;
    }

    public void saveParameter(String configurationId, String key, Object param) {
        saveParameter(configurationId, key, param, false);
    }

    public void saveParameter(String configurationId, String key, Object param,
            boolean insertOnly) {
        final String updatePrefixSql = "update " + tablePrefix + "_" + TABLE
                + " set ";
        final String updateSuffixSql = " = ? where node_group_id=? and param_Key=? and param_Order=1 and param_Type=?";

        String column = null;
        int propertyType = 0;
        if (param instanceof String) {
            column = "stringValue";
            propertyType = GlobalParameterType.STRING.ordinal();
        } else if (param instanceof Integer) {
            column = "int_value";
            propertyType = GlobalParameterType.INTEGER.ordinal();

        } else if (param instanceof BigDecimal) {
            column = "decimal_value";
            propertyType = GlobalParameterType.BIGDECIMAL.ordinal();

        } else if (param instanceof Boolean) {
            column = "boolean_value";
            propertyType = GlobalParameterType.BOOLEAN.ordinal();
        } else {
            throw new UnsupportedOperationException();
        }

        int count = 0;

        if (!insertOnly) {
            count = jdbcTemplate.update(updatePrefixSql + column
                    + updateSuffixSql, new Object[] { param, configurationId,
                    key, propertyType });
        }

        if (count == 0) {
            // Then insert ...
            jdbcTemplate
                    .update(
                            "insert into "
                                    + tablePrefix + "_"
                                    + TABLE
                                    + " (node_group_id, param_Key, param_Order, param_Type, "
                                    + column + ") values(?, ?, 1, ?, ?)",
                            new Object[] { configurationId, key, propertyType,
                                    param });
        }
    }

    public void saveParameters(String configurationId,
            Map<String, Object> parameters) {
        Set<String> keys = parameters.keySet();
        for (String key : keys) {
            saveParameter(configurationId, key, parameters.get(key));
        }
    }

    public void populateDefautGlobalParametersIfNeeded() {
        Set<String> keys = defaultGlobalParameters.keySet();
        for (String key : keys) {
            try {
                saveParameter(GLOBAL_CONFIGURATION_ID, key,
                        defaultGlobalParameters.get(key), true);
            } catch (DataIntegrityViolationException ex) {
                logger.info(key
                        + " has already been defined at the global level.");
            }
        }
    }

    public void setDefaultGlobalParameters(
            Map<String, Object> defaultGlobalParameters) {
        this.defaultGlobalParameters = defaultGlobalParameters;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

}
