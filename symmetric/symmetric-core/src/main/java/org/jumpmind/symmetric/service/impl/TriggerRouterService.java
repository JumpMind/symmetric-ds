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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.config.ITriggerCreationListener;
import org.jumpmind.symmetric.config.TriggerFailureListener;
import org.jumpmind.symmetric.config.TriggerSelector;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.util.FormatUtils;

/**
 * @see ITriggerRouterService
 */
public class TriggerRouterService extends AbstractService implements ITriggerRouterService {

    private IClusterService clusterService;

    private IConfigurationService configurationService;

    private Map<String, Router> routersCache;

    private Map<String, Trigger> triggersCache;

    private Map<String, TriggerRoutersCache> triggerRouterCacheByNodeGroupId = new HashMap<String, TriggerRoutersCache>();

    private long triggerRouterCacheTime;

    private long routersCacheTime;

    private long triggersCacheTime;

    private List<ITriggerCreationListener> triggerCreationListeners;

    private TriggerFailureListener failureListener = new TriggerFailureListener();

    private IStatisticManager statisticManager;

    private List<String> extraConfigTables = new ArrayList<String>();

    /**
     * Cache the history for performance. History never changes and does not
     * grow big so this should be OK.
     */
    private HashMap<Integer, TriggerHistory> historyMap = new HashMap<Integer, TriggerHistory>();

    public TriggerRouterService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IClusterService clusterService,
            IConfigurationService configurationService, IStatisticManager statisticManager) {
        super(parameterService, symmetricDialect);
        this.clusterService = clusterService;
        this.configurationService = configurationService;
        this.statisticManager = statisticManager;
        this.addTriggerCreationListeners(this.failureListener);
        setSqlMap(new TriggerRouterServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public List<Trigger> getTriggers() {
        return sqlTemplate.query("select "
                + getSql("selectTriggersColumnList", "selectTriggersSql"), new TriggerMapper());
    }

    public boolean isTriggerBeingUsed(String triggerId) {
        return sqlTemplate.queryForInt(getSql("countTriggerRoutersByTriggerIdSql"), triggerId) > 0;
    }

    public boolean doesTriggerExist(String triggerId) {
        return sqlTemplate.queryForInt(getSql("countTriggerByTriggerIdSql"), triggerId) > 0;
    }

    public boolean doesTriggerExistForTable(String tableName) {
        return sqlTemplate.queryForInt(getSql("countTriggerByTableNameSql"), tableName) > 0;
    }

    public void deleteTrigger(Trigger trigger) {
        sqlTemplate.update(getSql("deleteTriggerSql"), (Object) trigger.getTriggerId());
    }

    protected void deleteTriggerHistory(TriggerHistory history) {
        sqlTemplate.update(getSql("deleteTriggerHistorySql"), history.getTriggerHistoryId());
    }

    public void createTriggersOnChannelForTables(String channelId, String catalogName,
            String schemaName, List<String> tables, String lastUpdateBy) {
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

    public void createTriggersOnChannelForTables(String channelId, Set<Table> tables,
            String lastUpdateBy) {
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
        sqlTemplate.update(getSql("inactivateTriggerHistorySql"),
                new Object[] { history.getErrorMessage(), history.getTriggerHistoryId() });
    }

    public Map<Long, TriggerHistory> getHistoryRecords() {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        sqlTemplate.query(getSql("allTriggerHistSql"), new TriggerHistoryMapper(retMap));
        return retMap;
    }

    protected boolean isTriggerNameInUse(String triggerId, String triggerName) {
        return sqlTemplate.queryForInt(getSql("selectTriggerNameInUseSql"), triggerName,
                triggerName, triggerName, triggerId) > 0;
    }

    public TriggerHistory findTriggerHistory(String sourceTableName) {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        sqlTemplate.query(getSql("allTriggerHistSql", "triggerHistBySourceTableWhereSql"),
                new TriggerHistoryMapper(retMap), new Object[] { sourceTableName },
                new int[] { Types.VARCHAR });
        if (retMap.size() > 0) {
            return retMap.values().iterator().next();
        } else {
            return null;
        }
    }

    public TriggerHistory getTriggerHistory(int histId) {
        TriggerHistory history = historyMap.get(histId);
        if (history == null && histId >= 0) {
            history = (TriggerHistory) sqlTemplate.queryForObject(getSql("triggerHistSql"),
                    new TriggerHistoryMapper(), histId);
            historyMap.put(histId, history);
        }
        return history;
    }

    public TriggerHistory getNewestTriggerHistoryForTrigger(String triggerId, String catalogName,
            String schemaName, String tableName) {
        List<TriggerHistory> triggerHistories = sqlTemplate.query(getSql("latestTriggerHistSql"),
                new TriggerHistoryMapper(), triggerId, tableName);
        for (TriggerHistory triggerHistory : triggerHistories) {
            if ((StringUtils.isBlank(catalogName) && StringUtils.isBlank(triggerHistory
                    .getSourceCatalogName()))
                    || (StringUtils.isNotBlank(catalogName) && catalogName.equals(triggerHistory
                            .getSourceCatalogName()))) {
                if ((StringUtils.isBlank(schemaName) && StringUtils.isBlank(triggerHistory
                        .getSourceSchemaName()))
                        || (StringUtils.isNotBlank(schemaName) && catalogName.equals(triggerHistory
                                .getSourceSchemaName()))) {
                    return triggerHistory;
                }
            }
        }
        return null;
    }

    /**
     * Get a list of trigger histories that are currently active
     */
    public List<TriggerHistory> getActiveTriggerHistories() {
        return sqlTemplate.query(getSql("allTriggerHistSql", "activeTriggerHistSql"),
                new TriggerHistoryMapper());
    }

    protected List<Trigger> buildTriggersForSymmetricTables(String version,
            String... tablesToExclude) {
        List<Trigger> triggers = new ArrayList<Trigger>();
        List<String> tables = new ArrayList<String>(TableConstants.getConfigTables(symmetricDialect
                .getTablePrefix()));
        if (extraConfigTables != null) {
            for (String extraTable : extraConfigTables) {
                tables.add(extraTable);
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

        for (String tableName : tables) {
            Trigger trigger = buildTriggerForSymmetricTable(tableName);
            triggers.add(trigger);
        }
        return triggers;
    }

    protected Trigger buildTriggerForSymmetricTable(String tableName) {
        boolean syncChanges = !TableConstants.getTablesThatDoNotSync(tablePrefix).contains(
                tableName)
                && parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION);
        Trigger trigger = new Trigger();
        trigger.setTriggerId(Integer.toString(Math.abs(tableName.hashCode())));
        trigger.setSyncOnDelete(syncChanges);
        trigger.setSyncOnInsert(syncChanges);
        trigger.setSyncOnUpdate(syncChanges);
        trigger.setSyncOnIncomingBatch(true);
        trigger.setSourceTableName(tableName);
        trigger.setChannelId(Constants.CHANNEL_CONFIG);
        // little trick to force the rebuild of SymmetricDS triggers every time
        // there is a new version of SymmetricDS
        trigger.setLastUpdateTime(new Date(Version.version().hashCode()));
        return trigger;
    }

    public List<TriggerRouter> buildTriggerRoutersForSymmetricTables(String version,
            NodeGroupLink nodeGroupLink, String... tablesToExclude) {
        int initialLoadOrder = 1;

        List<Trigger> triggers = buildTriggersForSymmetricTables(version, tablesToExclude);
        List<TriggerRouter> triggerRouters = new ArrayList<TriggerRouter>(triggers.size());

        for (int j = 0; j < triggers.size(); j++) {
            Trigger trigger = triggers.get(j);

            TriggerRouter triggerRouter = buildTriggerRoutersForSymmetricTables(version, trigger,
                    nodeGroupLink);
            triggerRouter.setInitialLoadOrder(initialLoadOrder++);
            triggerRouters.add(triggerRouter);
        }
        return triggerRouters;
    }

    protected TriggerRouter buildTriggerRoutersForSymmetricTables(String version, Trigger trigger,
            NodeGroupLink nodeGroupLink) {
        TriggerRouter triggerRouter = new TriggerRouter();
        triggerRouter.setTrigger(trigger);

        Router router = triggerRouter.getRouter();
        router.setRouterType("configurationChanged");
        router.setNodeGroupLink(nodeGroupLink);
        router.setLastUpdateTime(trigger.getLastUpdateTime());

        triggerRouter.setLastUpdateTime(trigger.getLastUpdateTime());

        if (trigger.getSourceTableName().equalsIgnoreCase(
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_TRIGGER))) {
            router.setRouterType("configurationChanged");
        }

        return triggerRouter;
    }

    private String getTriggerRouterSql() {
        return getTriggerRouterSql(null);
    }

    private String getTriggerRouterSql(String sql) {
        return getSql("select ", "selectTriggersColumnList", ",", "selectRoutersColumnList", ",",
                "selectTriggerRoutersColumnList", "selectTriggerRoutersSql", sql);
    }

    public List<TriggerRouter> getTriggerRouters() {
        return sqlTemplate.query(getTriggerRouterSql(), new TriggerRouterMapper());
    }

    public Set<TriggerRouter> getTriggerRouterForTableForCurrentNode(String catalogName,
            String schemaName, String tableName, boolean refreshCache) {
        return getTriggerRouterForTableForCurrentNode(null, catalogName, schemaName, tableName,
                refreshCache);
    }

    public Set<TriggerRouter> getTriggerRouterForTableForCurrentNode(NodeGroupLink link,
            String catalogName, String schemaName, String tableName, boolean refreshCache) {
        TriggerRoutersCache cache = getTriggerRoutersCacheForCurrentNode(refreshCache);
        Collection<List<TriggerRouter>> triggerRouters = cache.triggerRoutersByTriggerId.values();
        HashSet<TriggerRouter> returnList = new HashSet<TriggerRouter>();
        for (List<TriggerRouter> list : triggerRouters) {
            for (TriggerRouter triggerRouter : list) {
                if (isMatch(link, triggerRouter)
                        && isMatch(catalogName, schemaName, tableName, triggerRouter.getTrigger())) {
                    returnList.add(triggerRouter);
                }
            }
        }
        return returnList;
    }

    protected boolean isMatch(NodeGroupLink link, TriggerRouter router) {
        if (link != null && router != null && router.getRouter() != null) {
            return link.getSourceNodeGroupId().equals(
                    router.getRouter().getNodeGroupLink().getSourceNodeGroupId())
                    && link.getTargetNodeGroupId().equals(
                            router.getRouter().getNodeGroupLink().getTargetNodeGroupId());
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
        List<TriggerRouter> triggerRouters = new ArrayList<TriggerRouter>();
        List<NodeGroupLink> links = configurationService.getNodeGroupLinksFor(sourceNodeGroupId);
        for (NodeGroupLink nodeGroupLink : links) {
            triggerRouters.addAll(buildTriggerRoutersForSymmetricTables(Version.version(),
                    nodeGroupLink));
        }
        return triggerRouters;
    }

    protected void mergeInConfigurationTablesTriggerRoutersForCurrentNode(String sourceNodeGroupId,
            List<TriggerRouter> configuredInDatabase) {
        List<TriggerRouter> virtualConfigTriggers = getConfigurationTablesTriggerRoutersForCurrentNode(sourceNodeGroupId);
        for (TriggerRouter trigger : virtualConfigTriggers) {
            if (trigger.getRouter().getNodeGroupLink().getSourceNodeGroupId()
                    .equalsIgnoreCase(sourceNodeGroupId)
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

    public Map<String, List<TriggerRouter>> getTriggerRoutersForCurrentNode(boolean refreshCache) {
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

    protected TriggerRoutersCache getTriggerRoutersCacheForCurrentNode(boolean refreshCache) {
        String myNodeGroupId = parameterService.getNodeGroupId();
        long triggerRouterCacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS);
        TriggerRoutersCache cache = triggerRouterCacheByNodeGroupId == null ? null
                : triggerRouterCacheByNodeGroupId.get(myNodeGroupId);
        if (cache == null
                || refreshCache
                || System.currentTimeMillis() - this.triggerRouterCacheTime > triggerRouterCacheTimeoutInMs) {
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
                cache = triggerRouterCacheByNodeGroupId == null ? null
                        : triggerRouterCacheByNodeGroupId.get(myNodeGroupId);
            }
        }
        return cache;
    }

    /**
     * @see ITriggerRouterService#getActiveRouterByIdForCurrentNode(String,
     *      boolean)
     */
    public Router getActiveRouterByIdForCurrentNode(String routerId, boolean refreshCache) {
        return getTriggerRoutersCacheForCurrentNode(refreshCache).routersByRouterId.get(routerId);
    }

    /**
     * @see ITriggerRouterService#getRoutersByGroupLink(NodeGroupLink)
     */
    public List<Router> getRoutersByGroupLink(NodeGroupLink link) {
        return sqlTemplate.query(
                getSql("select", "selectRoutersColumnList", "selectRouterByNodeGroupLinkWhereSql"),
                new RouterMapper(), link.getSourceNodeGroupId(), link.getTargetNodeGroupId());
    }

    public Trigger getTriggerById(String triggerId) {
        return getTriggerById(triggerId, true);
    }

    public Trigger getTriggerById(String triggerId, boolean refreshCache) {
        final long triggerCacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS);
        Map<String, Trigger> cache = this.triggersCache;
        if (cache == null || !cache.containsKey(triggerId) || refreshCache
                || (System.currentTimeMillis() - this.triggersCacheTime) > triggerCacheTimeoutInMs) {
            synchronized (this) {
                this.triggersCacheTime = System.currentTimeMillis();
                List<Trigger> triggers = new ArrayList<Trigger>(getTriggers());
                triggers.addAll(buildTriggersForSymmetricTables(Version.version()));
                cache = new HashMap<String, Trigger>(triggers.size());
                for (Trigger trigger : triggers) {
                    cache.put(trigger.getTriggerId(), trigger);
                }
                this.triggersCache = cache;
            }
        }
        return cache.get(triggerId);
    }

    public Router getRouterById(String routerId) {
        return getRouterById(routerId, true);
    }

    public Router getRouterById(String routerId, boolean refreshCache) {
        final long routerCacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS);
        Map<String, Router> cache = this.routersCache;
        if (cache == null || refreshCache
                || System.currentTimeMillis() - this.routersCacheTime > routerCacheTimeoutInMs) {
            synchronized (this) {
                this.routersCacheTime = System.currentTimeMillis();
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
        return sqlTemplate.query(getSql("select ", "selectRoutersColumnList", "selectRoutersSql"),
                new RouterMapper());
    }

    public List<TriggerRouter> getAllTriggerRoutersForCurrentNode(String sourceNodeGroupId) {
        List<TriggerRouter> triggers = (List<TriggerRouter>) sqlTemplate.query(
                getTriggerRouterSql("activeTriggersForSourceNodeGroupSql"),
                new TriggerRouterMapper(), sourceNodeGroupId);
        mergeInConfigurationTablesTriggerRoutersForCurrentNode(sourceNodeGroupId, triggers);
        return triggers;
    }

    public List<TriggerRouter> getAllTriggerRoutersForReloadForCurrentNode(
            String sourceNodeGroupId, String targetNodeGroupId) {
        return (List<TriggerRouter>) sqlTemplate.query(
                getTriggerRouterSql("activeTriggersForReloadSql"), new TriggerRouterMapper(),
                sourceNodeGroupId, targetNodeGroupId, Constants.CHANNEL_CONFIG);
    }

    public TriggerRouter findTriggerRouterById(String triggerId, String routerId) {
        List<TriggerRouter> configs = (List<TriggerRouter>) sqlTemplate.query(
                getTriggerRouterSql("selectTriggerRouterSql"), new TriggerRouterMapper(),
                triggerId, routerId);
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            return null;
        }
    }

    public Map<String, List<TriggerRouter>> getTriggerRoutersByChannel(String nodeGroupId) {
        final Map<String, List<TriggerRouter>> retMap = new HashMap<String, List<TriggerRouter>>();
        sqlTemplate.query(getTriggerRouterSql("selectGroupTriggersSql"), new TriggerRouterMapper() {
            @Override
            public TriggerRouter mapRow(Row rs) {
                TriggerRouter tr = super.mapRow(rs);
                List<TriggerRouter> list = retMap.get(tr.getTrigger().getChannelId());
                if (list == null) {
                    list = new ArrayList<TriggerRouter>();
                    retMap.put(tr.getTrigger().getChannelId(), list);
                }
                list.add(tr);
                return tr;
            };
        }, nodeGroupId);
        return retMap;
    }

    public void insert(TriggerHistory newHistRecord) {
        sqlTemplate.update(
                getSql("insertTriggerHistorySql"),
                new Object[] { newHistRecord.getTriggerId(), newHistRecord.getSourceTableName(),
                        newHistRecord.getTableHash(), newHistRecord.getCreateTime(),
                        newHistRecord.getColumnNames(), newHistRecord.getPkColumnNames(),
                        newHistRecord.getLastTriggerBuildReason().getCode(),
                        newHistRecord.getNameForDeleteTrigger(),
                        newHistRecord.getNameForInsertTrigger(),
                        newHistRecord.getNameForUpdateTrigger(),
                        newHistRecord.getSourceSchemaName(), newHistRecord.getSourceCatalogName(),
                        newHistRecord.getTriggerRowHash(), newHistRecord.getErrorMessage() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.TIMESTAMP,
                        Types.VARCHAR, Types.VARCHAR, Types.CHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.VARCHAR });
    }

    public void deleteTriggerRouter(TriggerRouter triggerRouter) {
        sqlTemplate.update(getSql("deleteTriggerRouterSql"), (Object) triggerRouter.getTrigger()
                .getTriggerId(), triggerRouter.getRouter().getRouterId());
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
        if (0 == sqlTemplate.update(
                getSql("updateTriggerRouterSql"),
                new Object[] { triggerRouter.getInitialLoadOrder(),
                        triggerRouter.getInitialLoadSelect(),
                        triggerRouter.isPingBackEnabled() ? 1 : 0, triggerRouter.getLastUpdateBy(),
                        triggerRouter.getLastUpdateTime(),
                        triggerRouter.getTrigger().getTriggerId(),
                        triggerRouter.getRouter().getRouterId() }, new int[] { Types.NUMERIC,
                        Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.TIMESTAMP,
                        Types.VARCHAR, Types.VARCHAR })) {
            triggerRouter.setCreateTime(triggerRouter.getLastUpdateTime());
            sqlTemplate.update(
                    getSql("insertTriggerRouterSql"),
                    new Object[] { triggerRouter.getInitialLoadOrder(),
                            triggerRouter.getInitialLoadSelect(),
                            triggerRouter.isPingBackEnabled() ? 1 : 0,
                            triggerRouter.getCreateTime(), triggerRouter.getLastUpdateBy(),
                            triggerRouter.getLastUpdateTime(),
                            triggerRouter.getTrigger().getTriggerId(),
                            triggerRouter.getRouter().getRouterId() }, new int[] { Types.NUMERIC,
                            Types.VARCHAR, Types.SMALLINT, Types.TIMESTAMP, Types.VARCHAR,
                            Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR });
        }
    }

    protected void resetTriggerRouterCacheByNodeGroupId() {
        triggerRouterCacheTime = 0;
    }

    public void saveRouter(Router router) {
        router.setLastUpdateTime(new Date());
        router.nullOutBlankFields();
        if (0 == sqlTemplate
                .update(getSql("updateRouterSql"),
                        new Object[] { router.getTargetCatalogName(), router.getTargetSchemaName(),
                                router.getTargetTableName(),
                                router.getNodeGroupLink().getSourceNodeGroupId(),
                                router.getNodeGroupLink().getTargetNodeGroupId(),
                                router.getRouterType(), router.getRouterExpression(),
                                router.isSyncOnUpdate() ? 1 : 0, router.isSyncOnInsert() ? 1 : 0,
                                router.isSyncOnDelete() ? 1 : 0, router.getLastUpdateBy(),
                                router.getLastUpdateTime(), router.getRouterId() }, new int[] {
                                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                                Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.TIMESTAMP,
                                Types.VARCHAR })) {
            router.setCreateTime(router.getLastUpdateTime());
            sqlTemplate.update(
                    getSql("insertRouterSql"),
                    new Object[] { router.getTargetCatalogName(), router.getTargetSchemaName(),
                            router.getTargetTableName(),
                            router.getNodeGroupLink().getSourceNodeGroupId(),
                            router.getNodeGroupLink().getTargetNodeGroupId(),
                            router.getRouterType(), router.getRouterExpression(),
                            router.isSyncOnUpdate() ? 1 : 0, router.isSyncOnInsert() ? 1 : 0,
                            router.isSyncOnDelete() ? 1 : 0, router.getCreateTime(),
                            router.getLastUpdateBy(), router.getLastUpdateTime(),
                            router.getRouterId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                            Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR });
        }
        resetTriggerRouterCacheByNodeGroupId();
    }

    public boolean isRouterBeingUsed(String routerId) {
        return sqlTemplate.queryForInt(getSql("countTriggerRoutersByRouterIdSql"), routerId) > 0;
    }

    public void deleteRouter(Router router) {
        if (router != null) {
            sqlTemplate.update(getSql("deleteRouterSql"), (Object) router.getRouterId());
        }
    }

    public void saveTrigger(Trigger trigger) {
        trigger.setLastUpdateTime(new Date());
        trigger.nullOutBlankFields();
        if (0 == sqlTemplate.update(
                getSql("updateTriggerSql"),
                new Object[] { trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName(), trigger.getChannelId(),
                        trigger.isSyncOnUpdate() ? 1 : 0, trigger.isSyncOnInsert() ? 1 : 0,
                        trigger.isSyncOnDelete() ? 1 : 0, trigger.isSyncOnIncomingBatch() ? 1 : 0,
                        trigger.isUseStreamLobs() ? 1 : 0, trigger.isUseCaptureLobs() ? 1 : 0,
                        trigger.getNameForUpdateTrigger(), trigger.getNameForInsertTrigger(),
                        trigger.getNameForDeleteTrigger(), trigger.getSyncOnUpdateCondition(),
                        trigger.getSyncOnInsertCondition(), trigger.getSyncOnDeleteCondition(),
                        trigger.getTxIdExpression(), trigger.getExcludedColumnNames(),
                        trigger.getLastUpdateBy(), trigger.getLastUpdateTime(),
                        trigger.getExternalSelect(), trigger.getTriggerId() }, new int[] {
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT,
                        Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                        Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR })) {
            trigger.setCreateTime(trigger.getLastUpdateTime());
            sqlTemplate.update(
                    getSql("insertTriggerSql"),
                    new Object[] { trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                            trigger.getSourceTableName(), trigger.getChannelId(),
                            trigger.isSyncOnUpdate() ? 1 : 0, trigger.isSyncOnInsert() ? 1 : 0,
                            trigger.isSyncOnDelete() ? 1 : 0,
                            trigger.isSyncOnIncomingBatch() ? 1 : 0,
                            trigger.isUseStreamLobs() ? 1 : 0, trigger.isUseCaptureLobs() ? 1 : 0,
                            trigger.getNameForUpdateTrigger(), trigger.getNameForInsertTrigger(),
                            trigger.getNameForDeleteTrigger(), trigger.getSyncOnUpdateCondition(),
                            trigger.getSyncOnInsertCondition(), trigger.getSyncOnDeleteCondition(),
                            trigger.getTxIdExpression(), trigger.getExcludedColumnNames(),
                            trigger.getCreateTime(), trigger.getLastUpdateBy(),
                            trigger.getLastUpdateTime(), trigger.getExternalSelect(),
                            trigger.getTriggerId() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT,
                            Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                            Types.VARCHAR });
        }
    }

    public void syncTriggers() {
        syncTriggers(null, false);
    }

    public void syncTriggers(StringBuilder sqlBuffer, boolean genAlways) {
        if (clusterService.lock(ClusterConstants.SYNCTRIGGERS)) {
            synchronized (this) {
                try {
                    log.info("Synchronizing triggers");
                    // make sure channels are read from the database
                    configurationService.reloadChannels();
                    List<Trigger> triggersForCurrentNode = getTriggersForCurrentNode();
                    inactivateTriggers(triggersForCurrentNode, sqlBuffer);
                    updateOrCreateDatabaseTriggers(triggersForCurrentNode, sqlBuffer, genAlways);
                    resetTriggerRouterCacheByNodeGroupId();
                } finally {
                    clusterService.unlock(ClusterConstants.SYNCTRIGGERS);
                    log.info("Done synchronizing triggers");
                }
            }
        } else {
            log.info("Failed to synchronize trigger for {}");
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
                log.info("About to remove triggers for inactivated table: {}",
                        history.getSourceTableName());
                symmetricDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getNameForInsertTrigger(),
                        history.getSourceTableName(), history);
                symmetricDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getNameForDeleteTrigger(),
                        history.getSourceTableName(), history);
                symmetricDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getNameForUpdateTrigger(),
                        history.getSourceTableName(), history);

                if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                    if (this.triggerCreationListeners != null) {
                        for (ITriggerCreationListener l : this.triggerCreationListeners) {
                            l.triggerInactivated(null, history);
                        }
                    }
                }

                boolean triggerExists = symmetricDialect.doesTriggerExist(
                        history.getSourceCatalogName(), history.getSourceSchemaName(),
                        history.getSourceTableName(), history.getNameForInsertTrigger());
                triggerExists |= symmetricDialect.doesTriggerExist(history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getSourceTableName(),
                        history.getNameForUpdateTrigger());
                triggerExists |= symmetricDialect.doesTriggerExist(history.getSourceCatalogName(),
                        history.getSourceSchemaName(), history.getSourceTableName(),
                        history.getNameForDeleteTrigger());
                if (triggerExists) {
                    log.warn(
                            "There are triggers that have been marked as inactive.  Please remove triggers represented by trigger_id={} and trigger_hist_id={}",
                            history.getTriggerId(), history.getTriggerHistoryId());
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
        return new TriggerSelector(toList(getTriggerRoutersForCurrentNode(true).values())).select();
    }

    protected Set<Table> getTablesForTrigger(Trigger trigger, List<Trigger> triggers) {
        Set<Table> tables = new HashSet<Table>();

        if (trigger.isSourceTableNameWildcarded()) {
            Database database = symmetricDialect.getPlatform().readDatabase(
                    trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                    new String[] { "TABLE" });
            Table[] tableArray = database.getTables();
            for (Table table : tableArray) {
                if (FormatUtils.isWildCardMatch(table.getName(), trigger.getSourceTableName())
                        && !containsExactMatchForSourceTableName(table.getName(), triggers)
                        && !table.getName().toLowerCase().startsWith(tablePrefix)) {
                    tables.add(table);
                }
            }
        } else {
            Table table = symmetricDialect.getPlatform().getTableFromCache(
                    trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                    trigger.getSourceTableName(), true);
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    private boolean containsExactMatchForSourceTableName(String tableName, List<Trigger> triggers) {
        for (Trigger trigger : triggers) {
            if (trigger.getSourceTableName().equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    protected void updateOrCreateDatabaseTriggers(List<Trigger> triggers, StringBuilder sqlBuffer,
            boolean genAlways) {

        for (Trigger trigger : triggers) {
            TriggerHistory newestHistory = null;
            try {
                TriggerReBuildReason reason = TriggerReBuildReason.NEW_TRIGGERS;

                String errorMessage = null;
                Channel channel = configurationService.getChannel(trigger.getChannelId());
                if (channel == null) {
                    errorMessage = String
                            .format("Trigger %s had an unrecognized channel_id of '%s'.  Please check to make sure the channel exists.  Creating trigger on the '%s' channel",
                                    trigger.getTriggerId(), trigger.getChannelId(),
                                    Constants.CHANNEL_DEFAULT);
                    log.error(errorMessage);
                    trigger.setChannelId(Constants.CHANNEL_DEFAULT);
                }

                Set<Table> tables = getTablesForTrigger(trigger, triggers);
                if (tables.size() > 0) {
                    for (Table table : tables) {
                        TriggerHistory latestHistoryBeforeRebuild = getNewestTriggerHistoryForTrigger(
                                trigger.getTriggerId(), trigger.getSourceCatalogName(),
                                trigger.getSourceSchemaName(),
                                trigger.isSourceTableNameWildcarded() ? table.getName()
                                        : trigger.getSourceTableName());

                        boolean forceRebuildOfTriggers = false;
                        if (latestHistoryBeforeRebuild == null) {
                            reason = TriggerReBuildReason.NEW_TRIGGERS;
                            forceRebuildOfTriggers = true;

                        } else if (table.calculateTableHashcode() != latestHistoryBeforeRebuild
                                .getTableHash()) {
                            reason = TriggerReBuildReason.TABLE_SCHEMA_CHANGED;
                            forceRebuildOfTriggers = true;

                        } else if (trigger
                                .hasChangedSinceLastTriggerBuild(latestHistoryBeforeRebuild
                                        .getCreateTime())
                                || trigger.toHashedValue() != latestHistoryBeforeRebuild
                                        .getTriggerRowHash()) {
                            reason = TriggerReBuildReason.TABLE_SYNC_CONFIGURATION_CHANGED;
                            forceRebuildOfTriggers = true;
                        } else if (genAlways) {
                            reason = TriggerReBuildReason.FORCED;
                            forceRebuildOfTriggers = true;
                        }

                        boolean supportsTriggers = symmetricDialect.getPlatform().getPlatformInfo()
                                .isTriggersSupported();

                        newestHistory = rebuildTriggerIfNecessary(sqlBuffer,
                                forceRebuildOfTriggers, trigger, DataEventType.INSERT, reason,
                                latestHistoryBeforeRebuild, null, trigger.isSyncOnInsert()
                                        && supportsTriggers, table);

                        newestHistory = rebuildTriggerIfNecessary(sqlBuffer,
                                forceRebuildOfTriggers, trigger, DataEventType.UPDATE, reason,
                                latestHistoryBeforeRebuild, newestHistory, trigger.isSyncOnUpdate()
                                        && supportsTriggers, table);

                        newestHistory = rebuildTriggerIfNecessary(sqlBuffer,
                                forceRebuildOfTriggers, trigger, DataEventType.DELETE, reason,
                                latestHistoryBeforeRebuild, newestHistory, trigger.isSyncOnDelete()
                                        && supportsTriggers, table);

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
                    }

                } else {
                    log.error(
                            "The configured table does not exist in the datasource that is configured: {}",
                            trigger.qualifiedSourceTableName());

                    if (this.triggerCreationListeners != null) {
                        for (ITriggerCreationListener l : this.triggerCreationListeners) {
                            l.tableDoesNotExist(trigger);
                        }
                    }
                }
            } catch (Exception ex) {
                log.error(
                        String.format("Failed to create triggers for %s",
                                trigger.qualifiedSourceTableName()), ex);

                if (newestHistory != null) {
                    // Make sure all the triggers are removed from the table
                    symmetricDialect.removeTrigger(null, trigger.getSourceCatalogName(),
                            trigger.getSourceSchemaName(), newestHistory.getNameForInsertTrigger(),
                            trigger.getSourceTableName(), newestHistory);
                    symmetricDialect.removeTrigger(null, trigger.getSourceCatalogName(),
                            trigger.getSourceSchemaName(), newestHistory.getNameForUpdateTrigger(),
                            trigger.getSourceTableName(), newestHistory);
                    symmetricDialect.removeTrigger(null, trigger.getSourceCatalogName(),
                            trigger.getSourceSchemaName(), newestHistory.getNameForDeleteTrigger(),
                            trigger.getSourceTableName(), newestHistory);
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
        int maxTriggerNameLength = symmetricDialect.getMaxTriggerNameLength();

        newTriggerHist.setNameForInsertTrigger(getTriggerName(DataEventType.INSERT,
                maxTriggerNameLength, trigger, table).toUpperCase());
        newTriggerHist.setNameForUpdateTrigger(getTriggerName(DataEventType.UPDATE,
                maxTriggerNameLength, trigger, table).toUpperCase());
        newTriggerHist.setNameForDeleteTrigger(getTriggerName(DataEventType.DELETE,
                maxTriggerNameLength, trigger, table).toUpperCase());

        String oldTriggerName = null;
        String oldSourceSchema = null;
        String oldCatalogName = null;
        if (oldhist != null) {
            oldTriggerName = oldhist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = oldhist.getSourceSchemaName();
            oldCatalogName = oldhist.getSourceCatalogName();
            triggerExists = symmetricDialect.doesTriggerExist(oldCatalogName, oldSourceSchema,
                    oldhist.getSourceTableName(), oldTriggerName);
        } else {
            // We had no trigger_hist row, lets validate that the trigger as
            // defined in the trigger row data does not exist as well.
            oldTriggerName = newTriggerHist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = trigger.getSourceSchemaName();
            oldCatalogName = trigger.getSourceCatalogName();
            triggerExists = symmetricDialect.doesTriggerExist(oldCatalogName, oldSourceSchema,
                    trigger.getSourceTableName(), oldTriggerName);
        }

        if (!triggerExists && forceRebuild) {
            reason = TriggerReBuildReason.TRIGGERS_MISSING;
        }

        if ((forceRebuild || !triggerIsActive) && triggerExists) {
            symmetricDialect.removeTrigger(sqlBuffer, oldCatalogName, oldSourceSchema,
                    oldTriggerName, trigger.getSourceTableName(), oldhist);
            triggerExists = false;
            triggerRemoved = true;
        }

        boolean isDeadTrigger = !trigger.isSyncOnInsert() && !trigger.isSyncOnUpdate()
                && !trigger.isSyncOnDelete();

        if (hist == null
                && (oldhist == null || (!triggerExists && triggerIsActive) || (isDeadTrigger && forceRebuild))) {
            insert(newTriggerHist);
            hist = getNewestTriggerHistoryForTrigger(
                    trigger.getTriggerId(),
                    trigger.getSourceCatalogName(),
                    trigger.getSourceSchemaName(),
                    trigger.isSourceTableNameWildcarded() ? table.getName() : trigger
                            .getSourceTableName());
        }

        try {
            if (!triggerExists && triggerIsActive) {
                symmetricDialect
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
            if (!symmetricDialect.doesTriggerExist(hist.getSourceCatalogName(),
                    hist.getSourceSchemaName(), hist.getSourceTableName(),
                    hist.getTriggerNameForDmlType(dmlType))) {
                log.warn(
                        "Cleaning up trigger hist row of {} after failing to create the associated trigger",
                        hist.getTriggerHistoryId());
                hist.setErrorMessage(ex.getMessage());
                inactivateTriggerHistory(hist);
            }
            throw ex;
        }

        return hist;
    }

    protected static String replaceCharsForTriggerName(String triggerName) {
        return triggerName.replaceAll("[^a-zA-Z0-9_]|[a|e|i|o|u|A|E|I|O|U]", "");
    }

    protected String getTriggerName(DataEventType dml, int maxTriggerNameLength, Trigger trigger, Table table) {

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
            if (trigger.isSourceTableNameWildcarded()) {
              triggerSuffix2 = replaceCharsForTriggerName(table.getName());  
            }             
            String triggerSuffix3 = replaceCharsForTriggerName("_"
                    + parameterService.getNodeGroupId());
            triggerName = triggerPrefix1 + triggerSuffix1 + triggerSuffix2 + triggerSuffix3;
            // use the node group id as part of the trigger if we can because it
            // helps uniquely identify the trigger in embedded databases. In
            // hsqldb we choose the
            // correct connection based on the presence of a table that is named
            // for the trigger.
            // If the trigger isn't unique across all databases, then we can
            // choose the wrong connection.
            if (triggerName.length() > maxTriggerNameLength && maxTriggerNameLength > 0) {
                triggerName = triggerPrefix1 + triggerSuffix1 + triggerSuffix2;
            }
        }

        triggerName = triggerName.toUpperCase();

        if (triggerName.length() > maxTriggerNameLength && maxTriggerNameLength > 0) {
            triggerName = triggerName.substring(0, maxTriggerNameLength - 1);
            log.debug(
                    "We just truncated the trigger name for the {} trigger id={}.  You might want to consider manually providing a name for the trigger that is less than {} characters long",
                    new Object[] { dml.name().toLowerCase(), trigger.getTriggerId(),
                            maxTriggerNameLength });
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

    class TriggerHistoryMapper implements ISqlRowMapper<TriggerHistory> {
        Map<Long, TriggerHistory> retMap = null;

        TriggerHistoryMapper() {
        }

        TriggerHistoryMapper(Map<Long, TriggerHistory> map) {
            this.retMap = map;
        }

        public TriggerHistory mapRow(Row rs) {
            TriggerHistory hist = new TriggerHistory();
            hist.setTriggerHistoryId(rs.getInt("trigger_hist_id"));
            hist.setTriggerId(rs.getString("trigger_id"));
            hist.setSourceTableName(rs.getString("source_table_name"));
            hist.setTableHash(rs.getInt("table_hash"));
            hist.setCreateTime(rs.getDateTime("create_time"));
            hist.setPkColumnNames(rs.getString("pk_column_names"));
            hist.setColumnNames(rs.getString("column_names"));
            hist.setLastTriggerBuildReason(TriggerReBuildReason.fromCode(rs
                    .getString("last_trigger_build_reason")));
            hist.setNameForDeleteTrigger(rs.getString("name_for_delete_trigger"));
            hist.setNameForInsertTrigger(rs.getString("name_for_insert_trigger"));
            hist.setNameForUpdateTrigger(rs.getString("name_for_update_trigger"));
            hist.setSourceSchemaName(rs.getString("source_schema_name"));
            hist.setSourceCatalogName(rs.getString("source_catalog_name"));
            hist.setTriggerRowHash(rs.getLong("trigger_row_hash"));
            hist.setErrorMessage(rs.getString("error_message"));
            if (this.retMap != null) {
                this.retMap.put((long) hist.getTriggerHistoryId(), hist);
            }
            return hist;
        }
    }

    class RouterMapper implements ISqlRowMapper<Router> {
        public Router mapRow(Row rs) {
            Router router = new Router();
            router.setSyncOnInsert(rs.getBoolean("r_sync_on_insert"));
            router.setSyncOnUpdate(rs.getBoolean("r_sync_on_update"));
            router.setSyncOnDelete(rs.getBoolean("r_sync_on_delete"));
            router.setTargetCatalogName(rs.getString("target_catalog_name"));
            router.setNodeGroupLink(configurationService.getNodeGroupLinkFor(
                    rs.getString("source_node_group_id"), rs.getString("target_node_group_id")));
            router.setTargetSchemaName(rs.getString("target_schema_name"));
            router.setTargetTableName(rs.getString("target_table_name"));

            String condition = rs.getString("router_expression");
            if (!StringUtils.isBlank(condition)) {
                router.setRouterExpression(condition);
            }
            router.setRouterType(rs.getString("router_type"));
            router.setRouterId(rs.getString("router_id"));
            router.setCreateTime(rs.getDateTime("r_create_time"));
            router.setLastUpdateTime(rs.getDateTime("r_last_update_time"));
            router.setLastUpdateBy(rs.getString("r_last_update_by"));
            return router;
        }
    }

    class TriggerMapper implements ISqlRowMapper<Trigger> {
        public Trigger mapRow(Row rs) {
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

            trigger.setCreateTime(rs.getDateTime("t_create_time"));
            trigger.setLastUpdateTime(rs.getDateTime("t_last_update_time"));
            trigger.setLastUpdateBy(rs.getString("t_last_update_by"));
            trigger.setExcludedColumnNames(rs.getString("excluded_column_names"));

            return trigger;
        }
    }

    class TriggerRouterMapper implements ISqlRowMapper<TriggerRouter> {

        private TriggerMapper triggerMapper = new TriggerMapper();
        private RouterMapper routerMapper = new RouterMapper();

        public TriggerRouter mapRow(Row rs) {
            TriggerRouter triggerRouter = new TriggerRouter();

            triggerRouter.setTrigger(triggerMapper.mapRow(rs));
            triggerRouter.setRouter(routerMapper.mapRow(rs));

            triggerRouter.setCreateTime(rs.getDateTime("create_time"));
            triggerRouter.setLastUpdateTime(rs.getDateTime("last_update_time"));
            triggerRouter.setLastUpdateBy(rs.getString("last_update_by"));
            triggerRouter.setInitialLoadOrder(rs.getInt("initial_load_order"));
            triggerRouter.setInitialLoadSelect(rs.getString("initial_load_select"));
            triggerRouter.setPingBackEnabled(rs.getBoolean("ping_back_enabled"));

            return triggerRouter;
        }
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

    public void addExtraConfigTable(String table) {
        if (this.extraConfigTables == null) {
            this.extraConfigTables = new ArrayList<String>();
        }
        this.extraConfigTables.add(table);
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
