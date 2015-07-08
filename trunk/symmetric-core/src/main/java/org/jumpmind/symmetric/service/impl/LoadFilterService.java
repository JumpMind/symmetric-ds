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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.LoadFilter.LoadFilterType;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.ILoadFilterService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.FormatUtils;

public class LoadFilterService extends AbstractService implements ILoadFilterService {

    private Map<NodeGroupLink, Map<LoadFilterType, Map<String, List<LoadFilter>>>> loadFilterCacheByNodeGroupLink;

    private long lastCacheTimeInMs;

    private Date lastUpdateTime;

    private IConfigurationService configurationService;

    public LoadFilterService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IConfigurationService configurationService) {
        super(parameterService, symmetricDialect);
        this.configurationService = configurationService;
        setSqlMap(new LoadFilterServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public Map<LoadFilterType, Map<String, List<LoadFilter>>> findLoadFiltersFor(NodeGroupLink nodeGroupLink,
            boolean useCache) {

        // get the cache timeout
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_LOAD_FILTER_IN_MS);

        // if the cache is expired or the caller doesn't want to use the cache,
        // pull the data and refresh the cache
        synchronized (this) {
            if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                    || loadFilterCacheByNodeGroupLink == null || useCache == false) {
                refreshCache();
            }
        }

        if (loadFilterCacheByNodeGroupLink != null) {
            Map<LoadFilterType, Map<String, List<LoadFilter>>> loadFilters = loadFilterCacheByNodeGroupLink
                    .get(nodeGroupLink);
            return loadFilters;
        }
        return null;
    }

    protected void refreshCache() {

        // get the cache timeout
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_LOAD_FILTER_IN_MS);

        synchronized (this) {
            if (System.currentTimeMillis() - lastCacheTimeInMs >= cacheTimeoutInMs
                    || loadFilterCacheByNodeGroupLink == null) {

                loadFilterCacheByNodeGroupLink = new HashMap<NodeGroupLink, Map<LoadFilterType, Map<String, List<LoadFilter>>>>();
                List<LoadFilterNodeGroupLink> loadFilters = getLoadFiltersFromDB();
                boolean ignoreCase = this.parameterService
                        .is(ParameterConstants.DB_METADATA_IGNORE_CASE);

                for (LoadFilterNodeGroupLink loadFilter : loadFilters) {
                    NodeGroupLink nodeGroupLink = loadFilter.getNodeGroupLink();
                    if (nodeGroupLink != null) {
                        Map<LoadFilterType, Map<String, List<LoadFilter>>> loadFiltersByType= loadFilterCacheByNodeGroupLink
                                .get(nodeGroupLink);
                        if (loadFiltersByType == null) {
                            loadFiltersByType = new HashMap<LoadFilterType, Map<String, List<LoadFilter>>>();
                            loadFilterCacheByNodeGroupLink.put(nodeGroupLink, loadFiltersByType);
                        }
                        Map<String, List<LoadFilter>> loadFiltersByTable = loadFiltersByType.get(loadFilter.getLoadFilterType());
                        if (loadFiltersByTable == null) {
                            loadFiltersByTable = new HashMap<String, List<LoadFilter>>();
                            loadFiltersByType.put(loadFilter.getLoadFilterType(), loadFiltersByTable);
                        }
                        String tableName = loadFilter.getTargetTableName();
                        if (StringUtils.isBlank(tableName)) {
                            tableName = FormatUtils.WILDCARD;
                        } else if (ignoreCase) {
                            tableName = tableName.toUpperCase();
                        }
                        
                        String schemaName = loadFilter.getTargetSchemaName();
                        if (StringUtils.isBlank(schemaName)) {
                            schemaName = FormatUtils.WILDCARD;
                        } else if (ignoreCase) {
                            schemaName = schemaName.toUpperCase();
                        }

                        String catalogName = loadFilter.getTargetCatalogName();
                        if (StringUtils.isBlank(catalogName)) {
                            catalogName = FormatUtils.WILDCARD;
                        } else if (ignoreCase) {
                            catalogName = catalogName.toUpperCase();
                        }

                        String qualifiedName = Table.getFullyQualifiedTableName(
                                catalogName, schemaName, tableName);
                        List<LoadFilter> loadFiltersForTable = loadFiltersByTable.get(qualifiedName);
                        if (loadFiltersForTable == null) {
                            loadFiltersForTable = new ArrayList<LoadFilter>();
                            loadFiltersByTable.put(qualifiedName, loadFiltersForTable);
                        }
                        loadFiltersForTable.add(loadFilter);
                    }
                }
                lastCacheTimeInMs = System.currentTimeMillis();
            }
        }
    }

    private List<LoadFilterNodeGroupLink> getLoadFiltersFromDB() {

        return sqlTemplate.query(getSql("selectLoadFilterTable"), new LoadFilterMapper());

    }

    class LoadFilterMapper implements ISqlRowMapper<LoadFilterNodeGroupLink> {

        public LoadFilterNodeGroupLink mapRow(Row rs) {
            LoadFilterNodeGroupLink loadFilter = new LoadFilterNodeGroupLink();

            loadFilter.setLoadFilterId(rs.getString("load_filter_id"));
            loadFilter.setNodeGroupLink(configurationService.getNodeGroupLinkFor(
                    rs.getString("source_node_group_id"), rs.getString("target_node_group_id"), false));
            loadFilter.setTargetCatalogName(rs.getString("target_catalog_name"));
            loadFilter.setTargetSchemaName(rs.getString("target_schema_name"));
            loadFilter.setTargetTableName(rs.getString("target_table_name"));
            loadFilter.setFilterOnInsert(rs.getBoolean("filter_on_insert"));
            loadFilter.setFilterOnUpdate(rs.getBoolean("filter_on_update"));
            loadFilter.setFilterOnDelete(rs.getBoolean("filter_on_delete"));
            loadFilter.setBeforeWriteScript(rs.getString("before_write_script"));
            loadFilter.setAfterWriteScript(rs.getString("after_write_script"));
            loadFilter.setBatchCompleteScript(rs.getString("batch_complete_script"));
            loadFilter.setBatchCommitScript(rs.getString("batch_commit_script"));
            loadFilter.setBatchRollbackScript(rs.getString("batch_rollback_script"));
            loadFilter.setHandleErrorScript(rs.getString("handle_error_script"));
            loadFilter.setCreateTime(rs.getDateTime("create_time"));
            loadFilter.setLastUpdateBy(rs.getString("last_update_by"));
            loadFilter.setLastUpdateTime(rs.getDateTime("last_update_time"));
            loadFilter.setLoadFilterOrder(rs.getInt("load_filter_order"));
            loadFilter.setFailOnError(rs.getBoolean("fail_on_error"));

            try {
                loadFilter.setLoadFilterType(LoadFilter.LoadFilterType.valueOf(rs.getString(
                        "load_filter_type").toUpperCase()));
            } catch (RuntimeException ex) {
                log.warn(
                        "Invalid value provided for load_filter_type of '{}.'  Valid values are: {}",
                        rs.getString("load_filter_type"),
                        Arrays.toString(LoadFilter.LoadFilterType.values()));
                throw ex;
            }

            return loadFilter;
        }
    }

    public List<LoadFilterNodeGroupLink> getLoadFilterNodeGroupLinks() {
        return getLoadFiltersFromDB();
    }

    public void saveLoadFilter(LoadFilterNodeGroupLink loadFilter) {
        loadFilter.setLastUpdateTime(new Date());
        Object[] args = { loadFilter.getAfterWriteScript(), loadFilter.getBatchCommitScript(),
                loadFilter.getBatchCompleteScript(), loadFilter.getBatchRollbackScript(),
                loadFilter.getBeforeWriteScript(), loadFilter.getHandleErrorScript(),
                loadFilter.getLoadFilterOrder(),
                loadFilter.getLoadFilterType().name(),
                loadFilter.getNodeGroupLink().getSourceNodeGroupId(),
                loadFilter.getNodeGroupLink().getTargetNodeGroupId(),
                loadFilter.getTargetCatalogName(), loadFilter.getTargetSchemaName(),
                loadFilter.getTargetTableName(), loadFilter.isFilterOnInsert() ? 1 : 0,
                loadFilter.isFilterOnUpdate() ? 1 : 0, loadFilter.isFilterOnDelete() ? 1 : 0,
                loadFilter.isFailOnError() ? 1 : 0, loadFilter.getLastUpdateBy(),
                loadFilter.getLastUpdateTime(), loadFilter.getLoadFilterId() };
        if (sqlTemplate.update(getSql("updateLoadFilterSql"), args) == 0) {
            sqlTemplate.update(getSql("insertLoadFilterSql"), args);
        }
        clearCache();
    }

    public void deleteLoadFilter(String loadFilterId) {
        sqlTemplate.update(getSql("deleteLoadFilterSql"), loadFilterId);
        clearCache();
    }

    public boolean refreshFromDatabase() {
        Date date = sqlTemplate.queryForObject(getSql("selectMaxLastUpdateTime"), Date.class);
        if (date != null) {
            if (lastUpdateTime == null || lastUpdateTime.before(date)) {
                if (lastUpdateTime != null) {
                   log.info("Newer filter settings were detected");
                }
                lastUpdateTime = date;
                clearCache();
                return true;
            }
        }
        return false;
    }

    public void clearCache() {
        synchronized (this) {
            this.loadFilterCacheByNodeGroupLink = null;
        }
    }

    public static class LoadFilterNodeGroupLink extends LoadFilter {

        private static final long serialVersionUID = 1L;

        protected NodeGroupLink nodeGroupLink;

        public void setNodeGroupLink(NodeGroupLink nodeGroupLink) {
            this.nodeGroupLink = nodeGroupLink;
        }

        public NodeGroupLink getNodeGroupLink() {
            return nodeGroupLink;
        }
    }

}
