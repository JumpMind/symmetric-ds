/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.ITypedPropertiesFactory;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.DatabaseParameter;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * @see IParameterService
 */
public class ParameterService extends AbstractParameterService implements IParameterService {

    String tablePrefix;

    private ITypedPropertiesFactory factory;

    private ParameterServiceSqlMap sql;

    private ISqlTemplate sqlTemplate;

    private Date lastUpdateTime;

    public ParameterService(IDatabasePlatform platform, ITypedPropertiesFactory factory,
            String tablePrefix) {
        this.tablePrefix = tablePrefix;
        this.factory = factory;
        this.sql = new ParameterServiceSqlMap(platform, AbstractService.createSqlReplacementTokens(
                tablePrefix, platform.getDatabaseInfo().getDelimiterToken()));
        this.sqlTemplate = platform.getSqlTemplate();
    }

    public String getTablePrefix() {
        return this.tablePrefix;
    }

    public boolean refreshFromDatabase() {
        Date date = sqlTemplate.queryForObject(sql.getSql("selectMaxLastUpdateTime"), Date.class);
        if (date != null) {
            if (lastUpdateTime == null || lastUpdateTime.before(date)) {
                if (lastUpdateTime != null) {
                  log.info("Newer database parameters were detected");
                }
                lastUpdateTime = date;
                rereadParameters();
                return true;
            }
        }
        return false;
    }

    /**
     * Save a parameter that applies to {@link ParameterConstants#ALL} external
     * ids and all node groups.
     */
    public void saveParameter(String key, Object paramValue, String lastUpdateBy) {
        this.saveParameter(ParameterConstants.ALL, ParameterConstants.ALL, key, paramValue, lastUpdateBy);
    }

    public void saveParameter(String externalId, String nodeGroupId, String key, Object paramValue, String lastUpdateBy) {
        paramValue = paramValue != null ? paramValue.toString() : null;

        int count = sqlTemplate.update(sql.getSql("updateParameterSql"), new Object[] { paramValue, lastUpdateBy,
                externalId, nodeGroupId, key });

        if (count == 0) {
            sqlTemplate.update(sql.getSql("insertParameterSql"), new Object[] { externalId,
                    nodeGroupId, key, paramValue, lastUpdateBy });
        }

        rereadParameters();
    }

    public void deleteParameter(String externalId, String nodeGroupId, String key) {
        sqlTemplate.update(sql.getSql("deleteParameterSql"), externalId, nodeGroupId, key);
        rereadParameters();
    }

    public void saveParameters(String externalId, String nodeGroupId, Map<String, Object> parameters, String lastUpdateBy) {
        Set<String> keys = parameters.keySet();
        for (String key : keys) {
            saveParameter(externalId, nodeGroupId, key, parameters.get(key), lastUpdateBy);
        }
    }

    protected TypedProperties rereadDatabaseParameters(String externalId, String nodeGroupId) {
        final TypedProperties properties = new TypedProperties();
        sqlTemplate.query(sql.getSql("selectParametersSql"), new ISqlRowMapper<Object>() {
            public Object mapRow(Row row) {
                String value = row.getString("param_value");
                if (value != null) {
                    properties.setProperty(row.getString("param_key"), row.getString("param_value"));
                }
                return row;
            }
        }, externalId, nodeGroupId);
        return properties;
    }

    public boolean isRegistrationServer() {
        return StringUtils.isBlank(getRegistrationUrl())
                || getRegistrationUrl().equals(getSyncUrl());
    }


    protected TypedProperties rereadApplicationParameters() {
        TypedProperties p = this.factory.reload();
        p.putAll(systemProperties);
        p.putAll(rereadDatabaseParameters(p));
        return p;
    }

    public List<DatabaseParameter> getDatabaseParametersFor(String paramKey) {
        return sqlTemplate.query(sql.getSql("selectParametersByKeySql"),
                new DatabaseParameterMapper(), paramKey);
    }

    public TypedProperties getDatabaseParametersByNodeGroupId(String nodeGroupId) {
        return rereadDatabaseParameters(ParameterConstants.ALL, nodeGroupId);
    }

    class DatabaseParameterMapper implements ISqlRowMapper<DatabaseParameter> {
        public DatabaseParameter mapRow(Row row) {
            return new DatabaseParameter(row.getString("param_key"), row.getString("param_value"),
                    row.getString("external_id"), row.getString("node_group_id"));
        }
    }

}
