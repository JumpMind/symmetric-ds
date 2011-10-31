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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.Message;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.config.ITriggerCreationListener;
import org.jumpmind.symmetric.config.TriggerFailureListener;
import org.jumpmind.symmetric.config.TriggerSelector;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IExtraConfigTables;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

/**
 * @see ITriggerRouterService
 */
public class TriggerRouterService extends AbstractService implements ITriggerRouterService {

    private Map<String, List<String>> rootConfigChannelTableNames;

    private IClusterService clusterService;

    private IConfigurationService configurationService;
    
    private Map<String, Router> routersCache;

    private Map<String, TriggerRoutersCache> triggerRouterCacheByNodeGroupId = new HashMap<String, TriggerRoutersCache>();
    
    private long triggerRouterCacheTime;
    
    private long routersCacheTime;

    private List<ITriggerCreationListener> triggerCreationListeners;

    private TriggerFailureListener failureListener = new TriggerFailureListener();
    
    private IStatisticManager statisticManager;
    
    private List<IExtraConfigTables> extraConfigTables = new ArrayList<IExtraConfigTables>();
    
    /**
     * Cache the history for performance. History never changes and does not
     * grow big so this should be OK.
     */
    private HashMap<Integer, TriggerHistory> historyMap = new HashMap<Integer, TriggerHistory>();

    public TriggerRouterService() {
        this.addTriggerCreationListeners(this.failureListener);
    }

    public List<Trigger> getTriggers() {
        return jdbcTemplate.query("select " + getSql("selectTriggersColumnList", "selectTriggersSql"), new TriggerMapper());
    }
    
    public boolean isTriggerBeingUsed(String triggerId) {
        return jdbcTemplate.queryForInt(getSql("countTriggerRoutersByTriggerIdSql"), triggerId) > 0;
    }
    
    public boolean doesTriggerExist(String triggerId) {
        return jdbcTemplate.queryForInt(getSql("countTriggerByTriggerIdSql"), triggerId) > 0;
    }
    
    public boolean doesTriggerExistForTable(String tableName) {
        return jdbcTemplate.queryForInt(getSql("countTriggerByTableNameSql"), tableName) > 0;
    }
    
    public void deleteTrigger(Trigger trigger) {
        jdbcTemplate.update(getSql("deleteTriggerSql"), trigger.getTriggerId());
    }
    
    protected void deleteTriggerHistory(TriggerHistory history) {
        jdbcTemplate.update(getSql("deleteTriggerHistorySql"), history.getTriggerHistoryId());
    }
    
    public void createTriggersOnChannelForTables(String channelId, String catalogName, String schemaName, List<String> tables, String lastUpdateBy) {
        for (String table : tables) {
            Trigger trigger = new Trigger();
            trigger.setChannelId(channelId);
            trigger.setSourceCatalogName(catalogName);
            trigger.setSourceSchemaName(schemaName);
            trigger.setSourceTableName(table);
            trigger.setTriggerId(table);
            trigger.setLastUpdateBy(lastUpdateBy);
            trigger.setLastUpdateTime(new Date());
            trigger.setCreateTime(new Date());
            saveTrigger(trigger);
        }
    }
    
    public void createTriggersOnChannelForTables(String channelId, Set<Table> tables, String lastUpdateBy) {
        for (Table table : tables) {
            Trigger trigger = new Trigger();
            trigger.setChannelId(channelId);
            trigger.setSourceTableName(table.getName());
            trigger.setTriggerId(table.getName());
            trigger.setLastUpdateBy(lastUpdateBy);
            trigger.setLastUpdateTime(new Date());
            trigger.setCreateTime(new Date());
            saveTrigger(trigger);
        }
    }

    
    public void inactivateTriggerHistory(TriggerHistory history) {
        jdbcTemplate.update(getSql("inactivateTriggerHistorySql"), new Object[] { history.getErrorMessage(), history
                .getTriggerHistoryId() });
    }

    public Map<Long, TriggerHistory> getHistoryRecords() {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        jdbcTemplate.query(getSql("allTriggerHistSql"), new TriggerHistoryMapper(retMap));
        return retMap;
    }

    protected boolean isTriggerNameInUse(String triggerId, String triggerName) {
        return jdbcTemplate.queryForInt(getSql("selectTriggerNameInUseSql"), triggerName,
                triggerName, triggerName, triggerId) > 0;
    }

    public TriggerHistory findTriggerHistory(String sourceTableName) {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        jdbcTemplate.query(getSql("allTriggerHistSql","triggerHistBySourceTableWhereSql"), new Object[] { sourceTableName },
                new int[] { Types.VARCHAR }, new TriggerHistoryMapper(retMap));
        if (retMap.size() > 0) {
            return retMap.values().iterator().next();
        } else {
            return null;
        }
    }

    public TriggerHistory getTriggerHistory(int histId) {
        TriggerHistory history = historyMap.get(histId);
        if (history == null && histId >= 0) {
            try {
                history = (TriggerHistory) jdbcTemplate.queryForObject(getSql("triggerHistSql"),
                        new Object[] { histId }, new TriggerHistoryMapper());
                historyMap.put(histId, history);
            } catch (EmptyResultDataAccessException ex) {
            }
        }
        return history;
    }

    public TriggerHistory getNewestTriggerHistoryForTrigger(String triggerId) {
        try {
            return (TriggerHistory) jdbcTemplate.queryForObject(getSql("latestTriggerHistSql"),
                    new Object[] { triggerId }, new TriggerHistoryMapper());
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    /**
     * Get a list of trigger histories that are currently active
     */
    public List<TriggerHistory> getActiveTriggerHistories() {
        return jdbcTemplate.query(getSql("allTriggerHistSql",
                "activeTriggerHistSql"), new TriggerHistoryMapper());
    }
    
    private String getNewestVersionOfRootConfigChannelTableNames() {
        TreeSet<String> ordered = new TreeSet<String>(rootConfigChannelTableNames.keySet());
        return ordered.last();
    }
    
    private String getMajorVersion(String version) {
        String majorVersion = Integer.toString(Version.parseVersion(version)[0]);        
        List<String> tables = rootConfigChannelTableNames.get(majorVersion);
        if (tables == null) {
            String newestVersion = getNewestVersionOfRootConfigChannelTableNames();
            log.warn("TriggersDefaultVersionWarning", newestVersion, majorVersion);
            majorVersion = newestVersion;
        }
        return majorVersion;
    }

    public List<TriggerRouter> getTriggerRoutersForRegistration(String version,
            NodeGroupLink nodeGroupLink, String... tablesToExclude) {
        int initialLoadOrder = 1;
        String majorVersion = getMajorVersion(version);
        List<String> tables = new ArrayList<String>(rootConfigChannelTableNames.get(majorVersion));
        if (extraConfigTables != null) {
            for (IExtraConfigTables extraTables : extraConfigTables) {
                tables.addAll(extraTables.provideTableNames());
            }
        }
        
        if (tablesToExclude != null) {
            for (String tableToExclude : tablesToExclude) {
                String tablename = TableConstants.getTableName(tablePrefix, tableToExclude);
                if (!tables.remove(tablename)) {
                    if (!tables.remove(tablename.toUpperCase())) {
                        tables.remove(tablename.toLowerCase());
                    }
                }
            }
        }
        
        List<TriggerRouter> triggers = new ArrayList<TriggerRouter>(tables.size());
        for (int j = 0; j < tables.size(); j++) {
            String tableName = tables.get(j);
            boolean syncChanges = !TableConstants.getTablesThatDoNotSync(tablePrefix).contains(
                    tableName);
            TriggerRouter trigger = buildRegistrationTriggerRouter(version, tableName, syncChanges,
                    nodeGroupLink);
            trigger.setInitialLoadOrder(initialLoadOrder++);
            if (tableName.equalsIgnoreCase(TableConstants.getTableName(tablePrefix,
                    TableConstants.SYM_TRIGGER))) {
                trigger.getRouter().setRouterType("trigger");
            }
            triggers.add(trigger);
        }
        return triggers;
    }

    protected TriggerRouter buildRegistrationTriggerRouter(String version, String tableName,
            boolean syncChanges, NodeGroupLink nodeGroupLink) {
        boolean autoSyncConfig = parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION);

        TriggerRouter triggerRouter = new TriggerRouter();
        Trigger trigger = triggerRouter.getTrigger();
        trigger.setTriggerId(Integer.toString(Math.abs(tableName.hashCode())));
        trigger.setSyncOnDelete(syncChanges && autoSyncConfig);
        trigger.setSyncOnInsert(syncChanges && autoSyncConfig);
        trigger.setSyncOnUpdate(syncChanges && autoSyncConfig);
        trigger.setSyncOnIncomingBatch(true);
        trigger.setSourceTableName(tableName);
        trigger.setChannelId(Constants.CHANNEL_CONFIG);

        Router router = triggerRouter.getRouter();
        router.setRouterType("configurationChanged");
        router.setNodeGroupLink(nodeGroupLink);

        // little trick to force the rebuild of SymmetricDS triggers every time
        // there is a new version of SymmetricDS
        trigger.setLastUpdateTime(new Date(Version.version().hashCode()));
        router.setLastUpdateTime(trigger.getLastUpdateTime());
        
        triggerRouter.setLastUpdateTime(trigger.getLastUpdateTime());

        return triggerRouter;
    }
    
    private String getTriggerRouterSql() {
        return getTriggerRouterSql(null);
    }
    
    private String getTriggerRouterSql(String sql) {
        return getSql("select", "selectTriggersColumnList", ",", "selectRoutersColumnList", ",",
                "selectTriggerRoutersColumnList","selectTriggerRoutersSql", sql);
    }
    
    public List<TriggerRouter> getTriggerRouters() {
        return jdbcTemplate.query(getTriggerRouterSql(), new TriggerRouterMapper());
    }
    
    public Set<TriggerRouter> getTriggerRouterForTableForCurrentNode(String catalogName, String schemaName, String tableName, boolean refreshCache) {
        return getTriggerRouterForTableForCurrentNode(null, catalogName, schemaName, tableName, refreshCache);
    }
    
    public Set<TriggerRouter> getTriggerRouterForTableForCurrentNode(NodeGroupLink link, String catalogName, String schemaName, String tableName, boolean refreshCache) {
        TriggerRoutersCache cache = getTriggerRoutersCacheForCurrentNode(refreshCache);
        Collection<List<TriggerRouter>> triggerRouters = cache.triggerRoutersByTriggerId.values();
        HashSet<TriggerRouter> returnList = new HashSet<TriggerRouter>();
        for (List<TriggerRouter> list : triggerRouters) {
            for (TriggerRouter triggerRouter : list) {                
                if (isMatch(link, triggerRouter) && 
                        isMatch(catalogName, schemaName, tableName, triggerRouter.getTrigger())) {
                    returnList.add(triggerRouter);
                }
            }
        }
        return returnList;
    }
    
    protected boolean isMatch(NodeGroupLink link, TriggerRouter router) {
        if (link != null && router != null && router.getRouter() != null) {
            return link.getSourceNodeGroupId().equals(router.getRouter().getNodeGroupLink().getSourceNodeGroupId()) 
            && link.getTargetNodeGroupId().equals(router.getRouter().getNodeGroupLink().getTargetNodeGroupId());
        } else {
            return true;
        }
    }
    
    protected boolean isMatch(String catalogName, String schemaName, String tableName,
            Trigger trigger) {
        if (!StringUtils.isBlank(tableName) && !tableName.equals(trigger.getSourceTableName())) {
            return false;
        } else if (StringUtils.isBlank(tableName)
                && !StringUtils.isBlank(trigger.getSourceTableName())) {
            return false;
        } else if (!StringUtils.isBlank(catalogName)
                && !catalogName.equals(trigger.getSourceCatalogName())) {
            return false;
        } else if (StringUtils.isBlank(catalogName)
                && !StringUtils.isBlank(trigger.getSourceCatalogName())) {
            return false;
        } else if (!StringUtils.isBlank(schemaName)
                && !schemaName.equals(trigger.getSourceSchemaName())) {
            return false;
        } else if (StringUtils.isBlank(schemaName)
                && !StringUtils.isBlank(trigger.getSourceSchemaName())) {
            return false;
        } else {
            return true;
        }
    }


    /**
     * Create a list of {@link TriggerRouter} for the SymmetricDS tables that
     * should have triggers created for them on the current node.
     */
    protected List<TriggerRouter> getConfigurationTablesTriggerRoutersForCurrentNode(
            String sourceNodeGroupId) {
        List<TriggerRouter> triggers = new ArrayList<TriggerRouter>();
        List<NodeGroupLink> links = configurationService.getNodeGroupLinksFor(sourceNodeGroupId);
        for (NodeGroupLink nodeGroupLink : links) {
            triggers.addAll(getTriggerRoutersForRegistration(Version.version(), nodeGroupLink));
            if (nodeGroupLink.getDataEventAction().equals(NodeGroupLinkAction.P)) {
                triggers
                        .add(buildRegistrationTriggerRouter(Version.version(), TableConstants
                                .getTableName(tablePrefix, TableConstants.SYM_NODE_HOST), true,
                                nodeGroupLink));
                log.debug("TriggerHistCreating", TableConstants.getTableName(tablePrefix,
                        TableConstants.SYM_NODE_HOST));

            }
        }
        return triggers;
    }

    protected void mergeInConfigurationTablesTriggerRoutersForCurrentNode(String sourceNodeGroupId,
            List<TriggerRouter> configuredInDatabase) {
        List<TriggerRouter> virtualConfigTriggers = getConfigurationTablesTriggerRoutersForCurrentNode(sourceNodeGroupId);
        for (TriggerRouter trigger : virtualConfigTriggers) {
            if (trigger.getRouter().getNodeGroupLink().getSourceNodeGroupId().equalsIgnoreCase(sourceNodeGroupId)
                    && !doesTriggerRouterExistInList(configuredInDatabase, trigger)) {
                configuredInDatabase.add(trigger);
            }
        }
    }
    
    protected boolean doesTriggerRouterExistInList(List<TriggerRouter> triggerRouters,
            TriggerRouter triggerRouter) {
        for (TriggerRouter checkMe : triggerRouters) {
            if (checkMe.isSame(triggerRouter)) {
                return true;
            }
        }
        return false;
    }

    public Map<String, List<TriggerRouter>> getTriggerRoutersForCurrentNode(
            boolean refreshCache) {
        return getTriggerRoutersCacheForCurrentNode(refreshCache).triggerRoutersByTriggerId;
    }
    
    public List<Trigger> getTriggersForCurrentNode(boolean refreshCache) {
        Map<String, List<TriggerRouter>> triggerRouters = getTriggerRoutersForCurrentNode(refreshCache);
        List<Trigger> triggers = new ArrayList<Trigger>(triggerRouters.size());
        for (List<TriggerRouter> list : triggerRouters.values()) {
            if (list.size() > 0) {
                triggers.add(list.get(0).getTrigger());
            }
        }
        return triggers;
    }

    protected TriggerRoutersCache getTriggerRoutersCacheForCurrentNode(
            boolean refreshCache) {
        String myNodeGroupId = parameterService.getNodeGroupId();
        long triggerRouterCacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS);           
        TriggerRoutersCache cache = triggerRouterCacheByNodeGroupId == null ? null : triggerRouterCacheByNodeGroupId.get(myNodeGroupId);
        if (cache == null || 
                refreshCache || 
                System.currentTimeMillis()-this.triggerRouterCacheTime > triggerRouterCacheTimeoutInMs) {
            synchronized (this) {
                this.triggerRouterCacheTime = System.currentTimeMillis();
                Map<String, TriggerRoutersCache> newTriggerRouterCacheByNodeGroupId = new HashMap<String, TriggerRoutersCache>();                
                List<TriggerRouter> triggerRouters = getAllTriggerRoutersForCurrentNode(myNodeGroupId);
                Map<String, List<TriggerRouter>> triggerRoutersByTriggerId = new HashMap<String, List<TriggerRouter>>(
                        triggerRouters.size());
                Map<String, Router> routers = new HashMap<String, Router>(triggerRouters.size());
                for (TriggerRouter triggerRouter : triggerRouters) {
                    String triggerId = triggerRouter.getTrigger().getTriggerId();
                    List<TriggerRouter> list = triggerRoutersByTriggerId.get(triggerId);
                    if (list == null) {
                        list = new ArrayList<TriggerRouter>();
                        triggerRoutersByTriggerId.put(triggerId, list);
                    }
                    list.add(triggerRouter);
                    routers.put(triggerRouter.getRouter().getRouterId(), triggerRouter.getRouter());
                }

                newTriggerRouterCacheByNodeGroupId.put(myNodeGroupId, new TriggerRoutersCache(
                        triggerRoutersByTriggerId, routers));
                this.triggerRouterCacheByNodeGroupId = newTriggerRouterCacheByNodeGroupId;
                cache = triggerRouterCacheByNodeGroupId == null ? null : triggerRouterCacheByNodeGroupId.get(myNodeGroupId);
            }
        }
        return cache;
    }

    /**
     * @see ITriggerRouterService#getActiveRouterByIdForCurrentNode(String, boolean)
     */
    public Router getActiveRouterByIdForCurrentNode(String routerId, boolean refreshCache) {
        return getTriggerRoutersCacheForCurrentNode(refreshCache).routersByRouterId
                .get(routerId);
    }
    
    /**
     * @see ITriggerRouterService#getRoutersByGroupLink(NodeGroupLink)
     */
    public List<Router> getRoutersByGroupLink(NodeGroupLink link) {
        return jdbcTemplate.query(getSql("select","selectRoutersColumnList",
                "selectRouterByNodeGroupLinkWhereSql"), new RouterMapper(), link.getSourceNodeGroupId(), link.getTargetNodeGroupId());
    }
    
    public Router getRouterById(String routerId) {
        return getRouterById(routerId, true);
    }
    
    public Router getRouterById(String routerId, boolean refreshCache) {
        long routerCacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS);
        Map<String, Router> cache = this.routersCache;
        if (cache == null || refreshCache
                || System.currentTimeMillis() - this.routersCacheTime > routerCacheTimeoutInMs) {
            synchronized (this) {
                this.triggerRouterCacheTime = System.currentTimeMillis();
                List<Router> routers = getRouters();
                cache = new HashMap<String, Router>(routers.size());
                for (Router router : routers) {
                    cache.put(router.getRouterId(), router);
                }
                this.routersCache = cache;
            }
        }
        return cache.get(routerId);
    }
    
    public List<Router> getRouters() {
        return jdbcTemplate.query(getSql("select", "selectRoutersColumnList", "selectRoutersSql"),
                new RouterMapper());
    }

    public List<TriggerRouter> getAllTriggerRoutersForCurrentNode(String sourceNodeGroupId) {
        List<TriggerRouter> triggers = (List<TriggerRouter>) jdbcTemplate.query(
                getTriggerRouterSql("activeTriggersForSourceNodeGroupSql"),
                new Object[] { sourceNodeGroupId }, new TriggerRouterMapper());
        mergeInConfigurationTablesTriggerRoutersForCurrentNode(sourceNodeGroupId, triggers);
        return triggers;
    }

    public List<TriggerRouter> getAllTriggerRoutersForReloadForCurrentNode(
            String sourceNodeGroupId, String targetNodeGroupId) {
        return (List<TriggerRouter>) jdbcTemplate.query(getTriggerRouterSql("activeTriggersForReloadSql"), new Object[] { sourceNodeGroupId,
                targetNodeGroupId, Constants.CHANNEL_CONFIG }, new TriggerRouterMapper());
    }

    public TriggerRouter findTriggerRouterById(String triggerId, String routerId) {
        List<TriggerRouter> configs = (List<TriggerRouter>) jdbcTemplate.query(
                getTriggerRouterSql("selectTriggerRouterSql"), new Object[] {
                        triggerId, routerId }, new TriggerRouterMapper());
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            return null;
        }
    }

    public Trigger getTriggerById(String triggerId) {
        List<TriggerRouter> triggers = (List<TriggerRouter>) jdbcTemplate.query(
                getTriggerRouterSql("selectTriggerByIdSql"),
                new Object[] { triggerId }, new TriggerRouterMapper());
        if (triggers.size() > 0) {
            return triggers.get(0).getTrigger();
        } else {
            return null;
        }
    }

    public Map<String, List<TriggerRouter>> getTriggerRoutersByChannel(String nodeGroupId) {
        final Map<String, List<TriggerRouter>> retMap = new HashMap<String, List<TriggerRouter>>();
        jdbcTemplate.query(getTriggerRouterSql("selectGroupTriggersSql"),
                new Object[] { nodeGroupId }, new TriggerRouterMapper() {
                    public TriggerRouter mapRow(java.sql.ResultSet rs, int arg1)
                            throws java.sql.SQLException {
                        TriggerRouter config = (TriggerRouter) super.mapRow(rs, arg1);
                        List<TriggerRouter> list = retMap.get(config.getTrigger().getChannelId());
                        if (list == null) {
                            list = new ArrayList<TriggerRouter>();
                            retMap.put(config.getTrigger().getChannelId(), list);
                        }
                        list.add(config);
                        return config;
                    };
                });
        return retMap;
    }

    public void insert(TriggerHistory newHistRecord) {
        jdbcTemplate.update(getSql("insertTriggerHistorySql"), new Object[] {
                newHistRecord.getTriggerId(), newHistRecord.getSourceTableName(),
                newHistRecord.getTableHash(), newHistRecord.getCreateTime(),
                newHistRecord.getColumnNames(), newHistRecord.getPkColumnNames(),
                newHistRecord.getLastTriggerBuildReason().getCode(),
                newHistRecord.getNameForDeleteTrigger(), newHistRecord.getNameForInsertTrigger(),
                newHistRecord.getNameForUpdateTrigger(), newHistRecord.getSourceSchemaName(),
                newHistRecord.getSourceCatalogName(), newHistRecord.getTriggerRowHash(), newHistRecord.getErrorMessage() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.TIMESTAMP,
                        Types.VARCHAR, Types.VARCHAR, Types.CHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.VARCHAR });
    }
    
    public void deleteTriggerRouter(TriggerRouter triggerRouter) {
        jdbcTemplate.update(getSql("deleteTriggerRouterSql"), triggerRouter.getTrigger().getTriggerId(), 
                triggerRouter.getRouter().getRouterId());
    }

    public void saveTriggerRouter(TriggerRouter triggerRouter) {
        saveTriggerRouter(triggerRouter, false);
    }
    
    public void saveTriggerRouter(TriggerRouter triggerRouter, boolean updateTriggerRouterTableOnly) {
        if (!updateTriggerRouterTableOnly) {
            saveTrigger(triggerRouter.getTrigger());
            saveRouter(triggerRouter.getRouter());
        }
        triggerRouter.setLastUpdateTime(new Date());
        if (0 == jdbcTemplate.update(getSql("updateTriggerRouterSql"),
                new Object[] { triggerRouter.getInitialLoadOrder(),
                        triggerRouter.getInitialLoadSelect(), 
                        triggerRouter.isPingBackEnabled() ? 1 : 0, 
                        triggerRouter.getLastUpdateBy(),
                        triggerRouter.getLastUpdateTime(),
                        triggerRouter.getTrigger().getTriggerId(),
                        triggerRouter.getRouter().getRouterId() },
                new int[] { Types.INTEGER, Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.TIMESTAMP,
                        Types.VARCHAR, Types.VARCHAR })) {
            triggerRouter.setCreateTime(triggerRouter.getLastUpdateTime());
            jdbcTemplate.update(getSql("insertTriggerRouterSql"), new Object[] {
                    triggerRouter.getInitialLoadOrder(), triggerRouter.getInitialLoadSelect(),
                    triggerRouter.isPingBackEnabled() ? 1 : 0,
                    triggerRouter.getCreateTime(), triggerRouter.getLastUpdateBy(),
                    triggerRouter.getLastUpdateTime(), triggerRouter.getTrigger().getTriggerId(),
                    triggerRouter.getRouter().getRouterId() }, new int[] { Types.INTEGER,
                    Types.VARCHAR, Types.SMALLINT, Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                    Types.VARCHAR });
        }
    }
    
    protected void resetTriggerRouterCacheByNodeGroupId() {
        triggerRouterCacheTime = 0;
    }

    public void saveRouter(Router router) {
        router.setLastUpdateTime(new Date());
        router.nullOutBlankFields();
        if (0 == jdbcTemplate.update(getSql("updateRouterSql"), new Object[] {
                router.getTargetCatalogName(), router.getTargetSchemaName(),
                router.getTargetTableName(), router.getNodeGroupLink().getSourceNodeGroupId(),
                router.getNodeGroupLink().getTargetNodeGroupId(), router.getRouterType(),
                router.getRouterExpression(), router.isSyncOnUpdate() ? 1 : 0,
                router.isSyncOnInsert() ? 1 : 0, router.isSyncOnDelete() ? 1 : 0,
                router.getLastUpdateBy(), router.getLastUpdateTime(), router.getRouterId() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                        Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.TIMESTAMP,
                        Types.VARCHAR })) {
            router.setCreateTime(router.getLastUpdateTime());
            jdbcTemplate.update(getSql("insertRouterSql"), new Object[] {
                    router.getTargetCatalogName(), router.getTargetSchemaName(),
                    router.getTargetTableName(), router.getNodeGroupLink().getSourceNodeGroupId(),
                    router.getNodeGroupLink().getTargetNodeGroupId(), router.getRouterType(),
                    router.getRouterExpression(), router.isSyncOnUpdate() ? 1 : 0,
                    router.isSyncOnInsert() ? 1 : 0, router.isSyncOnDelete() ? 1 : 0,
                    router.getCreateTime(), router.getLastUpdateBy(), router.getLastUpdateTime(),
                    router.getRouterId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                    Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.TIMESTAMP, Types.VARCHAR,
                    Types.TIMESTAMP, Types.VARCHAR });
        }
        resetTriggerRouterCacheByNodeGroupId();
    }
    

    public boolean isRouterBeingUsed(String routerId) {
        return jdbcTemplate.queryForInt(getSql("countTriggerRoutersByRouterIdSql"), routerId) > 0;
    }
    
    public void deleteRouter(Router router) {
        if (router != null) {
            jdbcTemplate.update(getSql("deleteRouterSql"), router.getRouterId());
        }        
    }

    public void saveTrigger(Trigger trigger) {
        trigger.setLastUpdateTime(new Date());
        trigger.nullOutBlankFields();
        if (0 == jdbcTemplate.update(getSql("updateTriggerSql"), new Object[] {
                trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                trigger.getSourceTableName(), trigger.getChannelId(),
                trigger.isSyncOnUpdate() ? 1 : 0, trigger.isSyncOnInsert() ? 1 : 0,
                trigger.isSyncOnDelete() ? 1 : 0, trigger.isSyncOnIncomingBatch() ? 1 : 0,
                trigger.isUseStreamLobs() ? 1 : 0,
                trigger.isUseCaptureLobs() ? 1 : 0,
                trigger.getNameForUpdateTrigger(), trigger.getNameForInsertTrigger(),
                trigger.getNameForDeleteTrigger(), trigger.getSyncOnUpdateCondition(),
                trigger.getSyncOnInsertCondition(), trigger.getSyncOnDeleteCondition(),
                trigger.getTxIdExpression(), trigger.getExcludedColumnNames(),
                trigger.getLastUpdateBy(), trigger.getLastUpdateTime(),
                trigger.getExternalSelect(), trigger.getTriggerId() }, new int[] { Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT,
                Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR })) {
            trigger.setCreateTime(trigger.getLastUpdateTime());
            jdbcTemplate.update(getSql("insertTriggerSql"), new Object[] {
                    trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                    trigger.getSourceTableName(), trigger.getChannelId(),
                    trigger.isSyncOnUpdate() ? 1 : 0, trigger.isSyncOnInsert() ? 1 : 0,
                    trigger.isSyncOnDelete() ? 1 : 0, trigger.isSyncOnIncomingBatch() ? 1 : 0,
                    trigger.isUseStreamLobs() ? 1 : 0,
                    trigger.isUseCaptureLobs() ? 1 : 0,
                    trigger.getNameForUpdateTrigger(), trigger.getNameForInsertTrigger(),
                    trigger.getNameForDeleteTrigger(), trigger.getSyncOnUpdateCondition(),
                    trigger.getSyncOnInsertCondition(), trigger.getSyncOnDeleteCondition(),
                    trigger.getTxIdExpression(), trigger.getExcludedColumnNames(),
                    trigger.getCreateTime(), trigger.getLastUpdateBy(),
                    trigger.getLastUpdateTime(), trigger.getExternalSelect(),
                    trigger.getTriggerId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                    Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                    Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR });
        }
    }

    public void syncTriggers() {
        syncTriggers(null, false);
    }

    public void syncTriggers(StringBuilder sqlBuffer, boolean gen_always) {
        if (clusterService.lock(ClusterConstants.SYNCTRIGGERS)) {
            synchronized (this) {
                try {
                    log.info("TriggersSynchronizing");
                    // make sure channels are read from the database
                    configurationService.reloadChannels();
                    List<Trigger> triggersForCurrentNode = getTriggersForCurrentNode();
                    inactivateTriggers(triggersForCurrentNode, sqlBuffer);
                    updateOrCreateDatabaseTriggers(triggersForCurrentNode, sqlBuffer, gen_always);
                    resetTriggerRouterCacheByNodeGroupId();
                } finally {
                    clusterService.unlock(ClusterConstants.SYNCTRIGGERS);
                    log.info("TriggersSynchronized");
                }
            }
        } else {
            log.info("TriggersSynchronizingFailedLock");
        }
    }
    
    protected Set<String> getTriggerIdsFrom(List<Trigger> triggersThatShouldBeActive) {
        Set<String> triggerIds = new HashSet<String>(triggersThatShouldBeActive.size());
        for (Trigger trigger : triggersThatShouldBeActive) {
            triggerIds.add(trigger.getTriggerId());
        }
        return triggerIds;
    }

    protected void inactivateTriggers(List<Trigger> triggersThatShouldBeActive,
            StringBuilder sqlBuffer) {
        List<TriggerHistory> activeHistories = getActiveTriggerHistories();
        Set<String> triggerIdsThatShouldBeActive = getTriggerIdsFrom(triggersThatShouldBeActive);
        for (TriggerHistory history : activeHistories) {
            if (!triggerIdsThatShouldBeActive.contains(history.getTriggerId())) {
                log.info("TriggersRemoving", history.getSourceTableName());
                dbDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getNameForInsertTrigger(),
                        history.getSourceTableName(), history);
                dbDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getNameForDeleteTrigger(),
                        history.getSourceTableName(), history);
                dbDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getNameForUpdateTrigger(),
                        history.getSourceTableName(), history);

                if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                    if (this.triggerCreationListeners != null) {
                        for (ITriggerCreationListener l : this.triggerCreationListeners) {
                            l.triggerInactivated(null, history);
                        }
                    }
                }

                boolean triggerExists = dbDialect.doesTriggerExist(history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getSourceTableName(),
                        history.getNameForInsertTrigger());
                triggerExists |= dbDialect.doesTriggerExist(history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getSourceTableName(),
                        history.getNameForUpdateTrigger());
                triggerExists |= dbDialect.doesTriggerExist(history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getSourceTableName(),
                        history.getNameForDeleteTrigger());
                if (triggerExists) {
                    log.warn("TriggersRemovingFailed", history.getTriggerId(),
                            history.getTriggerHistoryId());
                } else {
                    inactivateTriggerHistory(history);
                }
            }
        }
    }

    protected List<TriggerRouter> toList(Collection<List<TriggerRouter>> source) {
        ArrayList<TriggerRouter> list = new ArrayList<TriggerRouter>();
        for (List<TriggerRouter> triggerRouters : source) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                list.add(triggerRouter);
            }
        }
        return list;
    }
    
    protected List<Trigger> getTriggersForCurrentNode() {
        return  new TriggerSelector(toList(getTriggerRoutersForCurrentNode(
                true).values()))
        .select();
    }

    protected void updateOrCreateDatabaseTriggers(List<Trigger> triggers, StringBuilder sqlBuffer, boolean gen_always) {
        
        for (Trigger trigger : triggers) {
            TriggerHistory newestHistory = null;
            try {
                TriggerReBuildReason reason = TriggerReBuildReason.NEW_TRIGGERS;

                Table table = dbDialect.getTable(trigger.getSourceCatalogName(), trigger
                        .getSourceSchemaName(), trigger.getSourceTableName(), false);

                String errorMessage = null;
                Channel channel = configurationService.getChannel(trigger.getChannelId());
                if (channel == null) {
                    errorMessage = Message.get("TriggerFoundWithInvalidChannelId", trigger.getTriggerId(), trigger.getChannelId(), Constants.CHANNEL_DEFAULT);
                    log.error("TriggerFoundWithInvalidChannelId", trigger.getTriggerId(), trigger.getChannelId(), Constants.CHANNEL_DEFAULT);
                    trigger.setChannelId(Constants.CHANNEL_DEFAULT);
                }
                
                if (table != null) {
                    TriggerHistory latestHistoryBeforeRebuild = getNewestTriggerHistoryForTrigger(trigger
                            .getTriggerId());

                    boolean forceRebuildOfTriggers = false;
                    if (latestHistoryBeforeRebuild == null) {
                        reason = TriggerReBuildReason.NEW_TRIGGERS;
                        forceRebuildOfTriggers = true;

                    } else if (TriggerHistory.calculateTableHashFor(table) != latestHistoryBeforeRebuild
                            .getTableHash()) {
                        reason = TriggerReBuildReason.TABLE_SCHEMA_CHANGED;
                        forceRebuildOfTriggers = true;

                    } else if (trigger.hasChangedSinceLastTriggerBuild(latestHistoryBeforeRebuild
                            .getCreateTime())
                            || trigger.toHashedValue() != latestHistoryBeforeRebuild
                                    .getTriggerRowHash()) {
                        reason = TriggerReBuildReason.TABLE_SYNC_CONFIGURATION_CHANGED;
                        forceRebuildOfTriggers = true;
                    } else if (gen_always) {
                        reason = TriggerReBuildReason.FORCED;
                        forceRebuildOfTriggers = true;
                    }
                    
                    boolean supportsTriggers = dbDialect.getPlatform().getPlatformInfo().isTriggersSupported();

                    newestHistory = rebuildTriggerIfNecessary(sqlBuffer,
                            forceRebuildOfTriggers, trigger, DataEventType.INSERT, reason,
                            latestHistoryBeforeRebuild, null, trigger.isSyncOnInsert() && supportsTriggers, table);

                    newestHistory = rebuildTriggerIfNecessary(sqlBuffer, forceRebuildOfTriggers,
                            trigger, DataEventType.UPDATE, reason, latestHistoryBeforeRebuild,
                            newestHistory, trigger.isSyncOnUpdate() && supportsTriggers, table);

                    newestHistory = rebuildTriggerIfNecessary(sqlBuffer, forceRebuildOfTriggers,
                            trigger, DataEventType.DELETE, reason, latestHistoryBeforeRebuild,
                            newestHistory, trigger.isSyncOnDelete() && supportsTriggers, table);                    

                    if (latestHistoryBeforeRebuild != null && newestHistory != null) {
                        inactivateTriggerHistory(latestHistoryBeforeRebuild);
                    }

                    if (newestHistory != null) {
                        newestHistory.setErrorMessage(errorMessage);
                        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                            if (this.triggerCreationListeners != null) {
                                for (ITriggerCreationListener l : this.triggerCreationListeners) {
                                    l.triggerCreated(trigger, newestHistory);
                                }
                            }
                        }
                    }

                } else {
                    log.error("TriggerTableMissing", trigger.qualifiedSourceTableName());

                    if (this.triggerCreationListeners != null) {
                        for (ITriggerCreationListener l : this.triggerCreationListeners) {
                            l.tableDoesNotExist(trigger);
                        }
                    }
                }
            } catch (Exception ex) {
                log.error("TriggerSynchronizingFailed", ex, trigger.qualifiedSourceTableName());
                
                if (newestHistory != null) {
                    // Make sure all the triggers are removed from the table
                    dbDialect.removeTrigger(null, trigger.getSourceCatalogName(), trigger.getSourceSchemaName(), newestHistory.getNameForInsertTrigger(), trigger.getSourceTableName(), newestHistory);
                    dbDialect.removeTrigger(null, trigger.getSourceCatalogName(), trigger.getSourceSchemaName(), newestHistory.getNameForUpdateTrigger(), trigger.getSourceTableName(), newestHistory);
                    dbDialect.removeTrigger(null, trigger.getSourceCatalogName(), trigger.getSourceSchemaName(), newestHistory.getNameForDeleteTrigger(), trigger.getSourceTableName(), newestHistory);
                }
                
                if (this.triggerCreationListeners != null) {
                    for (ITriggerCreationListener l : this.triggerCreationListeners) {
                        l.triggerFailed(trigger, ex);
                    }
                }
            }

        }
    }

    protected TriggerHistory rebuildTriggerIfNecessary(StringBuilder sqlBuffer,
            boolean forceRebuild, Trigger trigger, DataEventType dmlType,
            TriggerReBuildReason reason, TriggerHistory oldhist, TriggerHistory hist,
            boolean triggerIsActive, Table table) {

        boolean triggerExists = false;
        boolean triggerRemoved = false;

        TriggerHistory newTriggerHist = new TriggerHistory(table, trigger, reason);
        int maxTriggerNameLength = dbDialect.getMaxTriggerNameLength();

        newTriggerHist.setNameForInsertTrigger(getTriggerName(DataEventType.INSERT,
                maxTriggerNameLength, trigger).toUpperCase());
        newTriggerHist.setNameForUpdateTrigger(getTriggerName(DataEventType.UPDATE,
                maxTriggerNameLength, trigger).toUpperCase());
        newTriggerHist.setNameForDeleteTrigger(getTriggerName(DataEventType.DELETE,
                maxTriggerNameLength, trigger).toUpperCase());

        String oldTriggerName = null;
        String oldSourceSchema = null;
        String oldCatalogName = null;
        if (oldhist != null) {
            oldTriggerName = oldhist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = oldhist.getSourceSchemaName();
            oldCatalogName = oldhist.getSourceCatalogName();
            triggerExists = dbDialect.doesTriggerExist(oldCatalogName, oldSourceSchema, oldhist
                    .getSourceTableName(), oldTriggerName);
        } else {
            // We had no trigger_hist row, lets validate that the trigger as
            // defined in the trigger row data does not exist as well.
            oldTriggerName = newTriggerHist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = trigger.getSourceSchemaName();
            oldCatalogName = trigger.getSourceCatalogName();
            triggerExists = dbDialect.doesTriggerExist(oldCatalogName, oldSourceSchema, trigger
                    .getSourceTableName(), oldTriggerName);
        }

        if (!triggerExists && forceRebuild) {
            reason = TriggerReBuildReason.TRIGGERS_MISSING;
        }

        if ((forceRebuild || !triggerIsActive) && triggerExists) {
            dbDialect.removeTrigger(sqlBuffer, oldCatalogName, oldSourceSchema, oldTriggerName,
                    trigger.getSourceTableName(), oldhist);
            triggerExists = false;
            triggerRemoved = true;
        }

        boolean isDeadTrigger = !trigger.isSyncOnInsert() && !trigger.isSyncOnUpdate()
                && !trigger.isSyncOnDelete();

        if (hist == null
                && (oldhist == null || (!triggerExists && triggerIsActive) || (isDeadTrigger && forceRebuild))) {
            insert(newTriggerHist);
            hist = getNewestTriggerHistoryForTrigger(trigger.getTriggerId());
        }
        
        try {
            if (!triggerExists && triggerIsActive) {
                dbDialect
                        .createTrigger(sqlBuffer, dmlType, trigger, hist,
                                configurationService.getChannel(trigger.getChannelId()),
                                tablePrefix, table);
                if (triggerRemoved) {
                    statisticManager.incrementTriggersRebuiltCount(1);
                } else {
                    statisticManager.incrementTriggersCreatedCount(1);
                }
            } else if (triggerRemoved) {
                statisticManager.incrementTriggersRemovedCount(1);
            }

        } catch (RuntimeException ex) {
            if (!dbDialect.doesTriggerExist(hist.getSourceCatalogName(),
                    hist.getSourceSchemaName(), hist.getSourceTableName(),
                    hist.getTriggerNameForDmlType(dmlType))) {
                log.warn("TriggerHistCleanup", hist.getTriggerHistoryId());
                hist.setErrorMessage(ex.getMessage());
                inactivateTriggerHistory(hist);
            }
            throw ex;
        }

        return hist;
    }
    
    protected String replaceCharsForTriggerName(String triggerName) {
        return triggerName.replaceAll(
                "[^a-zA-Z0-9_]|[a|e|i|o|u|A|E|I|O|U]", "");
    }

    protected String getTriggerName(DataEventType dml, int maxTriggerNameLength, Trigger trigger) {

        String triggerName = null;
        switch (dml) {
        case INSERT:
            if (!StringUtils.isBlank(trigger.getNameForInsertTrigger())) {
                triggerName = trigger.getNameForInsertTrigger();
            }
            break;
        case UPDATE:
            if (!StringUtils.isBlank(trigger.getNameForUpdateTrigger())) {
                triggerName = trigger.getNameForUpdateTrigger();
            }
            break;
        case DELETE:
            if (!StringUtils.isBlank(trigger.getNameForDeleteTrigger())) {
                triggerName = trigger.getNameForDeleteTrigger();
            }
            break;
        }

        if (StringUtils.isBlank(triggerName)) {
            String triggerPrefix1 = tablePrefix + "_";
            String triggerSuffix1 = "on_" + dml.getCode().toLowerCase() + "_for_";
            String triggerSuffix2 = replaceCharsForTriggerName(trigger.getTriggerId());
            String triggerSuffix3 = replaceCharsForTriggerName("_" + parameterService.getNodeGroupId());
            triggerName = triggerPrefix1 + triggerSuffix1 + triggerSuffix2 + triggerSuffix3;
            // use the node group id as part of the trigger if we can because it
            // helps uniquely identify the trigger in embedded databases. In hsqldb we choose the
            // correct connection based on the presence of a table that is named for the trigger. 
            // If the trigger isn't unique across all databases, then we can
            // choose the wrong connection.
            if (triggerName.length() > maxTriggerNameLength && maxTriggerNameLength > 0) {
                triggerName = triggerPrefix1 + triggerSuffix1 + triggerSuffix2;
            }
        }

        triggerName = triggerName.toUpperCase();

        if (triggerName.length() > maxTriggerNameLength && maxTriggerNameLength > 0) {
            triggerName = triggerName.substring(0, maxTriggerNameLength - 1);
            log.debug("TriggerNameTruncated", dml.name().toLowerCase(), trigger.getTriggerId(),
                    maxTriggerNameLength);
        }

        int duplicateCount = 0;
        while (isTriggerNameInUse(trigger.getTriggerId(), triggerName)) {
            duplicateCount++;
            String duplicateSuffix = Integer.toString(duplicateCount);
            if (triggerName.length() + duplicateSuffix.length() > maxTriggerNameLength) {
                triggerName = triggerName.substring(0,
                        triggerName.length() - duplicateSuffix.length())
                        + duplicateSuffix;
            } else {
                triggerName = triggerName + duplicateSuffix;
            }
        }

        return triggerName;
    }

    class NodeGroupLinkMapper implements RowMapper<NodeGroupLink> {
        public NodeGroupLink mapRow(ResultSet rs, int num) throws SQLException {
            NodeGroupLink node_groupTarget = new NodeGroupLink();
            node_groupTarget.setSourceNodeGroupId(rs.getString(1));
            node_groupTarget.setTargetNodeGroupId(rs.getString(2));
            node_groupTarget.setDataEventAction(NodeGroupLinkAction.fromCode(rs.getString(3)));
            return node_groupTarget;
        }
    }

    class TriggerHistoryMapper implements RowMapper<TriggerHistory> {
        Map<Long, TriggerHistory> retMap = null;

        TriggerHistoryMapper() {
        }

        TriggerHistoryMapper(Map<Long, TriggerHistory> map) {
            this.retMap = map;
        }

        public TriggerHistory mapRow(ResultSet rs, int i) throws SQLException {
            TriggerHistory hist = new TriggerHistory();
            hist.setTriggerHistoryId(rs.getInt(1));
            hist.setTriggerId(rs.getString(2));
            hist.setSourceTableName(rs.getString(3));
            hist.setTableHash(rs.getInt(4));
            hist.setCreateTime(rs.getTimestamp(5));
            hist.setPkColumnNames(rs.getString(6));
            hist.setColumnNames(rs.getString(7));
            hist.setLastTriggerBuildReason(TriggerReBuildReason.fromCode(rs.getString(8)));
            hist.setNameForDeleteTrigger(rs.getString(9));
            hist.setNameForInsertTrigger(rs.getString(10));
            hist.setNameForUpdateTrigger(rs.getString(11));
            hist.setSourceSchemaName(rs.getString(12));
            hist.setSourceCatalogName(rs.getString(13));
            hist.setTriggerRowHash(rs.getLong(14));
            hist.setErrorMessage(rs.getString(15));
            if (this.retMap != null) {
                this.retMap.put((long) hist.getTriggerHistoryId(), hist);
            }
            return hist;
        }
    }
    
    class RouterMapper implements RowMapper<Router> {        
        public Router mapRow(ResultSet rs, int rowNum) throws SQLException {
            Router router = new Router();
            router.setSyncOnInsert(rs.getBoolean("r_sync_on_insert"));
            router.setSyncOnUpdate(rs.getBoolean("r_sync_on_update"));
            router.setSyncOnDelete(rs.getBoolean("r_sync_on_delete"));
            router.setTargetCatalogName(rs.getString("target_catalog_name"));
            router.setNodeGroupLink(
            configurationService.getNodeGroupLinkFor(rs.getString("source_node_group_id"), rs.getString("target_node_group_id")));
            router.setTargetSchemaName(rs.getString("target_schema_name"));
            router.setTargetTableName(rs.getString("target_table_name"));

            String condition = rs.getString("router_expression");
            if (!StringUtils.isBlank(condition)) {
                router.setRouterExpression(condition);
            }
            router.setRouterType(rs.getString("router_type"));
            router.setRouterId(rs.getString("router_id"));
            router.setCreateTime(rs.getTimestamp("r_create_time"));
            router.setLastUpdateTime(rs.getTimestamp("r_last_update_time"));
            router.setLastUpdateBy(rs.getString("r_last_update_by"));
            return router;
        }
    }
    
    class TriggerMapper implements RowMapper<Trigger> {
        public Trigger mapRow(ResultSet rs, int rowNum) throws SQLException {
            Trigger trigger = new Trigger();
            trigger.setTriggerId(rs.getString("trigger_id"));
            trigger.setChannelId(rs.getString("channel_id"));
            trigger.setSourceTableName(rs.getString("source_table_name"));
            trigger.setSyncOnInsert(rs.getBoolean("sync_on_insert"));
            trigger.setSyncOnUpdate(rs.getBoolean("sync_on_update"));
            trigger.setSyncOnDelete(rs.getBoolean("sync_on_delete"));
            trigger.setSyncOnIncomingBatch(rs.getBoolean("sync_on_incoming_batch"));
            trigger.setUseStreamLobs(rs.getBoolean("use_stream_lobs"));
            trigger.setUseCaptureLobs(rs.getBoolean("use_capture_lobs"));
            trigger.setNameForDeleteTrigger(rs.getString("name_for_delete_trigger"));
            trigger.setNameForInsertTrigger(rs.getString("name_for_insert_trigger"));
            trigger.setNameForUpdateTrigger(rs.getString("name_for_update_trigger"));
            String schema = rs.getString("source_schema_name");
            trigger.setSourceSchemaName(schema);
            String catalog = rs.getString("source_catalog_name");
            trigger.setSourceCatalogName(catalog);
            String condition = rs.getString("sync_on_insert_condition");
            if (!StringUtils.isBlank(condition)) {
                trigger.setSyncOnInsertCondition(condition);
            }
            condition = rs.getString("sync_on_update_condition");
            if (!StringUtils.isBlank(condition)) {
                trigger.setSyncOnUpdateCondition(condition);
            }

            condition = rs.getString("sync_on_delete_condition");
            if (!StringUtils.isBlank(condition)) {
                trigger.setSyncOnDeleteCondition(condition);
            }

            condition = rs.getString("external_select");
            if (!StringUtils.isBlank(condition)) {
                trigger.setExternalSelect(condition);
            }

            trigger.setTxIdExpression(rs.getString("tx_id_expression"));
            
            trigger.setCreateTime(rs.getTimestamp("t_create_time"));
            trigger.setLastUpdateTime(rs.getTimestamp("t_last_update_time"));
            trigger.setLastUpdateBy(rs.getString("t_last_update_by"));
            trigger.setExcludedColumnNames(rs.getString("excluded_column_names"));

            return trigger;
        }
    }

    class TriggerRouterMapper implements RowMapper<TriggerRouter> {
        
        private TriggerMapper triggerMapper = new TriggerMapper();
        private RouterMapper routerMapper = new RouterMapper();
        
        public TriggerRouter mapRow(java.sql.ResultSet rs, int rowNum) throws java.sql.SQLException {
            TriggerRouter triggerRouter = new TriggerRouter();

            triggerRouter.setTrigger(triggerMapper.mapRow(rs, rowNum));
            triggerRouter.setRouter(routerMapper.mapRow(rs, rowNum));
       
            triggerRouter.setCreateTime(rs.getTimestamp("create_time"));
            triggerRouter.setLastUpdateTime(rs.getTimestamp("last_update_time"));
            triggerRouter.setLastUpdateBy(rs.getString("last_update_by"));
            triggerRouter.setInitialLoadOrder(rs.getInt("initial_load_order"));
            triggerRouter.setInitialLoadSelect(rs.getString("initial_load_select"));
            triggerRouter.setPingBackEnabled(rs.getBoolean("ping_back_enabled"));

            return triggerRouter;
        }
    }

    public void setRootConfigChannelTableNames(Map<String, List<String>> configChannelTableNames) {
        this.rootConfigChannelTableNames = configChannelTableNames;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setTriggerCreationListeners(
            List<ITriggerCreationListener> autoTriggerCreationListeners) {
        if (triggerCreationListeners != null) {
            for (ITriggerCreationListener l : triggerCreationListeners) {
                addTriggerCreationListeners(l);
            }
        }
    }

    public void addTriggerCreationListeners(ITriggerCreationListener l) {
        if (this.triggerCreationListeners == null) {
            this.triggerCreationListeners = new ArrayList<ITriggerCreationListener>();
        }
        this.triggerCreationListeners.add(l);
    }
    
    public void addExtraConfigTables(IExtraConfigTables extension) {
        this.extraConfigTables.add(extension);
    }

    public Map<Trigger, Exception> getFailedTriggers() {
        return this.failureListener.getFailures();
    }
    
    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }

    class TriggerRoutersCache {

        public TriggerRoutersCache(Map<String, List<TriggerRouter>> triggerRoutersByTriggerId,
                Map<String, Router> routersByRouterId) {
            this.triggerRoutersByTriggerId = triggerRoutersByTriggerId;
            this.routersByRouterId = routersByRouterId;
        }

        Map<String, List<TriggerRouter>> triggerRoutersByTriggerId = new HashMap<String, List<TriggerRouter>>();
        Map<String, Router> routersByRouterId = new HashMap<String, Router>();
    }
    
}