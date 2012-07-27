package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.ILoadFilterService;
import org.jumpmind.symmetric.service.IParameterService;

public class LoadFilterService extends AbstractService implements ILoadFilterService {

    private Map<NodeGroupLink,Map<String, List<LoadFilter>>> loadFilterCacheByNodeGroupLink;
	
	private long lastCacheTimeInMs;
    
    private IConfigurationService configurationService;

    public LoadFilterService(IParameterService parameterService, ISymmetricDialect symmetricDialect, IConfigurationService configurationService) {
        super(parameterService, symmetricDialect);
        this.configurationService = configurationService;
        setSqlMap(new LoadFilterServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));        
    }
	
	public Map<String, List<LoadFilter>> findLoadFiltersFor(NodeGroupLink nodeGroupLink,
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
            Map<String, List<LoadFilter>> loadFilters = loadFilterCacheByNodeGroupLink
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

            	loadFilterCacheByNodeGroupLink = new HashMap<NodeGroupLink, Map<String, List<LoadFilter>>>();
                List<LoadFilterNodeGroupLink> loadFilters = getLoadFiltersFromDB();
                
                for (LoadFilterNodeGroupLink loadFilter : loadFilters) {
                    NodeGroupLink nodeGroupLink = loadFilter.getNodeGroupLink();
                    if (nodeGroupLink != null) {                    	
	                    Map<String, List<LoadFilter>> loadFiltersByNodeGroup = loadFilterCacheByNodeGroupLink
	                    		.get(nodeGroupLink);
	                    if (loadFiltersByNodeGroup == null) {
	                    	loadFiltersByNodeGroup = new HashMap<String, List<LoadFilter>>();
	                    }
	                    List<LoadFilter> loadFiltersForTable = loadFiltersByNodeGroup.get(loadFilter.getTargetTableName());
	                    if (loadFiltersForTable == null) {
	                    	loadFiltersForTable = new ArrayList<LoadFilter>();
	                    }
	                    loadFiltersForTable.add(loadFilter);
	                    loadFiltersByNodeGroup.put(loadFilter.getTargetTableName(), loadFiltersForTable);
	                    loadFilterCacheByNodeGroupLink.put(nodeGroupLink,  loadFiltersByNodeGroup);
                    }
                }                
                lastCacheTimeInMs = System.currentTimeMillis();
            }
        }
    }	
	
    private List<LoadFilterNodeGroupLink> getLoadFiltersFromDB() {

    	return sqlTemplate.query(
                getSql("selectLoadFilterTable"), new LoadFilterMapper());

    }    
    
    class LoadFilterMapper implements ISqlRowMapper<LoadFilterNodeGroupLink> {
        
        public LoadFilterNodeGroupLink mapRow(Row rs) {
            LoadFilterNodeGroupLink loadFilter = new LoadFilterNodeGroupLink();

            loadFilter.setLoadFilterId(rs.getString("load_filter_id"));
            loadFilter.setNodeGroupLink(configurationService.getNodeGroupLinkFor(
                    rs.getString("source_node_group_id"), rs.getString("target_node_group_id")));            
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
            loadFilter.setCreateTime(rs.getDateTime("create_time"));
            loadFilter.setLastUpdateBy(rs.getString("last_update_by"));
            loadFilter.setLastUpdateTime(rs.getDateTime("last_update_time"));
            loadFilter.setLoadFilterOrder(rs.getInt("load_filter_order"));
            loadFilter.setFailOnError(rs.getBoolean("fail_on_error"));

            try {
                loadFilter.setLoadFilterType(LoadFilter.LoadFilterType
                		.valueOf(rs.getString("load_filter_type").toUpperCase()));
            } catch (RuntimeException ex) {
                log.warn(
                        "Invalid value provided for load_filter_type of '{}.'  Valid values are: {}",
                        rs.getString("load_filter_type"), Arrays.toString(LoadFilter.LoadFilterType.values()));
                throw ex;
            }
            
            return loadFilter;
        }
    }    
    
	public List<LoadFilter> getLoadFilters() {
		// TODO Auto-generated method stub
		return null;
	}

	public void saveLoadFilter(LoadFilter loadFilter) {
		// TODO Auto-generated method stub

	}

	public void deleteLoadFilter(String loadFilterId) {
		// TODO Auto-generated method stub

	}

	public void resetCache() {
		// TODO Auto-generated method stub

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
