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
package org.jumpmind.symmetric.load;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterFilterAdapter;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An out of the box filter that checks to see if the SymmetricDS configuration
 * has changed. If it has, it will take the correct action to apply the
 * configuration change to the current node.
 */
public class ConfigurationChangedDatabaseWriterFilter extends DatabaseWriterFilterAdapter implements
        IBuiltInExtensionPoint, ILoadSyncLifecycleListener {

    static final Logger log = LoggerFactory.getLogger(ConfigurationChangedDatabaseWriterFilter.class);

    final String CTX_KEY_RESYNC_NEEDED = "Resync."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FLUSH_GROUPLETS_NEEDED = "FlushGrouplets."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FLUSH_LOADFILTERS_NEEDED = "FlushLoadFilters."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_RESYNC_TABLE_NEEDED = "Resync.Table"
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();    

    final String CTX_KEY_FLUSH_CHANNELS_NEEDED = "FlushChannels."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_TRANSFORMS_NEEDED = "FlushTransforms."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_PARAMETERS_NEEDED = "FlushParameters."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_FLUSH_CONFLICTS_NEEDED = "FlushConflicts."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_NODE_SECURITY_NEEDED = "FlushNodeSecurity."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FLUSH_NODE_NEEDED = "FlushNode."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_RESTART_JOBMANAGER_NEEDED = "RestartJobManager."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();
    
    final String CTX_KEY_REINITIALIZED = "Reinitialized."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_FILE_SYNC_ENABLED = "FileSyncEnabled."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_INITIAL_LOAD_COMPLETED = "InitialLoadCompleted."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    final String CTX_KEY_INITIAL_LOAD_LISTENER = "InitialLoadListener."
            + ConfigurationChangedDatabaseWriterFilter.class.getSimpleName() + hashCode();

    private ISymmetricEngine engine;

    public ConfigurationChangedDatabaseWriterFilter(ISymmetricEngine engine) {
        this.engine = engine;
    }
    
    @Override
    public boolean beforeWrite(DataContext context, Table table, CsvData data) {
        IParameterService parameterService = engine.getParameterService();
        if (context.getBatch().getBatchId() == Constants.VIRTUAL_BATCH_FOR_REGISTRATION) {
            if (parameterService.is(ParameterConstants.REGISTRATION_REINITIALIZE_ENABLED)
                    && !Boolean.TRUE.equals(context.get(CTX_KEY_REINITIALIZED))) {
                log.info("Reinitializing the database because the {} parameter was set to true",
                        ParameterConstants.REGISTRATION_REINITIALIZE_ENABLED);
                engine.uninstall();
                engine.setupDatabase(true);
                engine.start();
                context.put(CTX_KEY_REINITIALIZED, Boolean.TRUE);
            }
        }

        checkReloadStarted(context, table, data);

        return true;
    }

    @Override
    public void afterWrite(DataContext context, Table table, CsvData data) {
        recordSyncNeeded(context, table, data);
        recordGroupletFlushNeeded(context, table);
        recordLoadFilterFlushNeeded(context, table);
        recordChannelFlushNeeded(context, table);
        recordTransformFlushNeeded(context, table);
        recordParametersFlushNeeded(context, table);
        recordJobManagerRestartNeeded(context, table, data);
        recordConflictFlushNeeded(context, table);
        recordNodeSecurityFlushNeeded(context, table);
        recordNodeFlushNeeded(context, table, data);
        recordFileSyncEnabled(context, table, data);
    }

    private void checkReloadStarted(DataContext context, Table table, CsvData data) {
        if (data.getDataEventType() == DataEventType.INSERT || data.getDataEventType() == DataEventType.UPDATE) {
            if (matchesTable(table, TableConstants.SYM_NODE_SECURITY)) {

                if (hasClientReloadListener(context)) {
                    Map<String, String> newData = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);
                    String initialLoadEnabled = newData.get("INITIAL_LOAD_ENABLED");
                    String nodeId = newData.get("NODE_ID");
    
                    INodeService nodeService = engine.getNodeService();
                    if (nodeId.equals(nodeService.findIdentityNodeId()) || nodeService.findIdentityNodeId() == null) {
                        boolean duringInitialLoad = nodeService.findIdentityNodeId() != null
                                && nodeService.findNodeSecurity(nodeService.findIdentityNodeId(), true).isInitialLoadEnabled();
                        if (!duringInitialLoad && "1".equals(initialLoadEnabled)) {
    
                            log.info("Reload started");
    
                            List<IClientReloadListener> listeners = engine.getExtensionService().getExtensionPointList(IClientReloadListener.class);
                            for (IClientReloadListener listener : listeners) {
                                listener.reloadStarted();
                            }
                        }
                    }
                }
            }
        }
    }

    private boolean hasClientReloadListener(DataContext context) {
        Boolean hasListener = (Boolean) context.get(CTX_KEY_INITIAL_LOAD_LISTENER);
        if (hasListener == null) {
            hasListener = engine.getExtensionService().getExtensionPointList(IClientReloadListener.class).size() > 0;
            context.put(CTX_KEY_INITIAL_LOAD_LISTENER, hasListener);
        }
        return hasListener;
    }
    
    private void recordGroupletFlushNeeded(DataContext context, Table table) {
        if (isGroupletFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_GROUPLETS_NEEDED, true);
        }
    }

    private void recordLoadFilterFlushNeeded(DataContext context, Table table) {
        if (isLoadFilterFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_LOADFILTERS_NEEDED, true);
        }
    }
    
    private void recordSyncNeeded(DataContext context, Table table, CsvData data) {
        if (isSyncTriggersNeeded(context, table)) {
            context.put(CTX_KEY_RESYNC_NEEDED, true);
        }
        
        if (data.getDataEventType() == DataEventType.CREATE) {   
            @SuppressWarnings("unchecked")
            Set<Table> tables = (Set<Table>)context.get(CTX_KEY_RESYNC_TABLE_NEEDED);
            if (tables == null) {
                tables = new HashSet<Table>();
                context.put(CTX_KEY_RESYNC_TABLE_NEEDED, tables);
            }
            tables.add(table);
        }
        
        if (data.getDataEventType() == DataEventType.UPDATE) {
            if (matchesTable(table, TableConstants.SYM_NODE_SECURITY)) {
                Map<String, String> newData = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);
                String initialLoadEnabled = newData.get("INITIAL_LOAD_ENABLED");
                String initialLoadTime = newData.get("INITIAL_LOAD_TIME");
                String nodeId = newData.get("NODE_ID");
                boolean isInitialLoadComplete = nodeId != null && nodeId.equals(context.getBatch().getTargetNodeId()) && StringUtils.isNotBlank(initialLoadTime)
                        && "0".equals(initialLoadEnabled);

                if (isInitialLoadComplete && !engine.getParameterService().is(ParameterConstants.TRIGGER_CREATE_BEFORE_INITIAL_LOAD)) {
                    log.info("Requesting syncTriggers because {} is false and sym_node_security changed to indicate that an initial load has completed",
                            ParameterConstants.TRIGGER_CREATE_BEFORE_INITIAL_LOAD);
                    context.put(CTX_KEY_RESYNC_NEEDED, true);
                    engine.getRegistrationService().setAllowClientRegistration(false);
                }
                
                if (isInitialLoadComplete && hasClientReloadListener(context)) {
                    INodeService nodeService = engine.getNodeService();
                    boolean duringInitialLoad = nodeService.findIdentityNodeId() != null
                            && nodeService.findNodeSecurity(nodeService.findIdentityNodeId(), true).isInitialLoadEnabled();
                    if (duringInitialLoad) {
                        context.put(CTX_KEY_INITIAL_LOAD_COMPLETED, true);
                    }
                }

            }
        }

    }

    private void recordJobManagerRestartNeeded(DataContext context, Table table, CsvData data) {
        if (isJobManagerRestartNeeded(table, data)) {
            context.put(CTX_KEY_RESTART_JOBMANAGER_NEEDED, true);
        }
    }
    
    private void recordConflictFlushNeeded(DataContext context, Table table) {
        if (isConflictFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_CONFLICTS_NEEDED, true);
        }
    }

    private void recordParametersFlushNeeded(DataContext context, Table table) {
        if (isParameterFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_PARAMETERS_NEEDED, true);
        }
    }

    private void recordChannelFlushNeeded(DataContext context, Table table) {
        if (isChannelFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_CHANNELS_NEEDED, true);
        }
    }

    private void recordTransformFlushNeeded(DataContext context, Table table) {
        if (isTransformFlushNeeded(table)) {
            context.put(CTX_KEY_FLUSH_TRANSFORMS_NEEDED, true);
        }
    }

    private void recordNodeSecurityFlushNeeded(DataContext context, Table table) {
        if (matchesTable(table, TableConstants.SYM_NODE_SECURITY)) {
            context.put(CTX_KEY_FLUSH_NODE_SECURITY_NEEDED, true);
        }
    }

    private void recordNodeFlushNeeded(DataContext context, Table table, CsvData data) {
        if (matchesTable(table, TableConstants.SYM_NODE) && 
                context.getBatch().getBatchId() != Constants.VIRTUAL_BATCH_FOR_REGISTRATION) {
            Map<String, String> newData = data.toColumnNameValuePairs(table.getColumnNames(), CsvData.ROW_DATA);
            String nodeId = newData.get("NODE_ID");
            Node node = engine.getNodeService().findNodeInCacheOnly(nodeId);
            if (node == null || data.getDataEventType() == DataEventType.INSERT || !node.isSyncEnabled()) {
                context.put(CTX_KEY_FLUSH_NODE_NEEDED, true);
            }
        }
    }
    
    private void recordFileSyncEnabled(DataContext context, Table table, CsvData data) {
        if (isFileSyncEnabled(table, data)) {
            context.put(CTX_KEY_FILE_SYNC_ENABLED, true);
        }
    }

    private boolean isSyncTriggersNeeded(DataContext context, Table table) {
        boolean autoSync = engine.getParameterService().is(ParameterConstants.AUTO_SYNC_TRIGGERS_AFTER_CONFIG_LOADED) || 
                context.getBatch().getBatchId() == Constants.VIRTUAL_BATCH_FOR_REGISTRATION;
        return autoSync && (matchesTable(table, TableConstants.SYM_TRIGGER)
                || matchesTable(table, TableConstants.SYM_ROUTER)
                || matchesTable(table, TableConstants.SYM_TRIGGER_ROUTER)
                || matchesTable(table, TableConstants.SYM_TRIGGER_ROUTER_GROUPLET)
                || matchesTable(table, TableConstants.SYM_GROUPLET_LINK)
                || matchesTable(table, TableConstants.SYM_NODE_GROUP_LINK));
    }
    
    private boolean isGroupletFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_GROUPLET_LINK) ||
                matchesTable(table, TableConstants.SYM_TRIGGER_ROUTER_GROUPLET) ||
                matchesTable(table, TableConstants.SYM_GROUPLET);
    }
    
    private boolean isLoadFilterFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_LOAD_FILTER);
    }    

    private boolean isChannelFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_CHANNEL);
    }
    
    private boolean isConflictFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_CONFLICT);
    }

    private boolean isParameterFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_PARAMETER);
    }

    private boolean isJobManagerRestartNeeded(Table table, CsvData data) {
        return matchesTable(table, TableConstants.SYM_PARAMETER)
                && data.getCsvData(CsvData.ROW_DATA) != null
                && data.getCsvData(CsvData.ROW_DATA).contains("job.");
    }
    
    private boolean isFileSyncEnabled(Table table, CsvData data) {
        return matchesTable(table, TableConstants.SYM_PARAMETER)
                && data.getCsvData(CsvData.ROW_DATA) != null
                && data.getCsvData(CsvData.ROW_DATA).contains(ParameterConstants.FILE_SYNC_ENABLE)
                && data.getCsvData(CsvData.ROW_DATA).contains("true");
    }

    private boolean isTransformFlushNeeded(Table table) {
        return matchesTable(table, TableConstants.SYM_TRANSFORM_COLUMN)
                || matchesTable(table, TableConstants.SYM_TRANSFORM_TABLE);
    }

    private boolean matchesTable(Table table, String tableSuffix) {
        if (table != null && table.getName() != null) {
            return table.getName().equalsIgnoreCase(
                    TableConstants.getTableName(engine.getParameterService().getTablePrefix(),
                            tableSuffix));
        } else {
            return false;
        }
    }
    
    public void syncStarted(DataContext context) {
    }
    
    public void syncEnded(DataContext context, List<IncomingBatch> batchesProcessed, Throwable ex) {

        IParameterService parameterService = engine.getParameterService();
        INodeService nodeService = engine.getNodeService();
        
        if (context.get(CTX_KEY_FLUSH_TRANSFORMS_NEEDED) != null) {
            log.info("About to refresh the cache of transformation because new configuration came through the data loader");
            engine.getTransformService().clearCache();
            log.info("About to clear the staging area because new transform configuration came through the data loader");
            engine.getStagingManager().clean(0);
            context.remove(CTX_KEY_FLUSH_TRANSFORMS_NEEDED);
        }

        if (context.get(CTX_KEY_RESTART_JOBMANAGER_NEEDED) != null
                || context.get(CTX_KEY_FILE_SYNC_ENABLED) != null) {
            IJobManager jobManager = engine.getJobManager();
            if (jobManager != null && jobManager.isStarted()) {
                log.info("About to restart jobs because new configuration came through the data loader");
                jobManager.restartJobs();                    
            }
            context.remove(CTX_KEY_RESTART_JOBMANAGER_NEEDED);
        }
        
        /**
         * No need to sync triggers until the entire sync process has finished just in case there
         * are multiple batches that contain configuration changes
         */
        if (context.get(CTX_KEY_RESYNC_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            log.info("About to syncTriggers because new configuration came through the data loader");
            engine.getClusterService().refreshLockEntries();  // Needed in case cluster.lock.enabled changed during config change.
            engine.getTriggerRouterService().syncTriggers();
            context.remove(CTX_KEY_RESYNC_NEEDED);
            engine.getRegistrationService().setAllowClientRegistration(true);
        }
        
        if (context.get(CTX_KEY_RESYNC_TABLE_NEEDED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)
                && (parameterService.is(ParameterConstants.TRIGGER_CREATE_BEFORE_INITIAL_LOAD)
                        || nodeService.findNodeSecurity(nodeService.findIdentityNodeId(), true).hasInitialLoaded())) {
            @SuppressWarnings("unchecked")
            Set<Table> tables = (Set<Table>)context.get(CTX_KEY_RESYNC_TABLE_NEEDED);
            for (Table table : tables) {
        		if (engine.getSymmetricDialect().getPlatform().equals(engine.getSymmetricDialect().getTargetPlatform())) {
        			engine.getTriggerRouterService().syncTriggers(table, false);
        		}
            }
            context.remove(CTX_KEY_RESYNC_TABLE_NEEDED);
        }    
        
        if (context.get(CTX_KEY_FILE_SYNC_ENABLED) != null
                && parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            log.info("About to syncTriggers for file snapshot because the file sync parameter has changed");
            engine.clearCaches();
            engine.getFileSyncService().clearCache();
            Table fileSnapshotTable = engine.getDatabasePlatform()
                    .getTableFromCache(TableConstants.getTableName(engine.getTablePrefix(), TableConstants.SYM_FILE_SNAPSHOT), false);
            engine.getTriggerRouterService().syncTriggers(fileSnapshotTable, false);
            
            
            context.remove(CTX_KEY_FILE_SYNC_ENABLED);
        }

        if (context.get(CTX_KEY_INITIAL_LOAD_COMPLETED) != null) {

            log.info("Reload completed");

            List<IClientReloadListener> listeners = engine.getExtensionService().getExtensionPointList(IClientReloadListener.class);
            for (IClientReloadListener listener : listeners) {
                listener.reloadCompleted();
            }

            context.remove(CTX_KEY_INITIAL_LOAD_COMPLETED);
        }
    }

    @Override
    public void batchCommitted(DataContext context) {
        
        IParameterService parameterService = engine.getParameterService();
        INodeService nodeService = engine.getNodeService();
        
        if (context.getBatch().getBatchId() == Constants.VIRTUAL_BATCH_FOR_REGISTRATION) {
            // mark registration as complete
            String nodeId = nodeService.findIdentityNodeId();
            if (nodeId != null) {
                NodeSecurity security = nodeService.findNodeSecurity(nodeId);
                if (security != null && 
                        (security.isRegistrationEnabled() || security.getRegistrationTime() == null)) {
                    engine.getRegistrationService().markNodeAsRegistered(nodeId);
                }
            }
        }       
        
        if (context.get(CTX_KEY_FLUSH_GROUPLETS_NEEDED) != null) {
            log.info("Grouplets flushed because new grouplet config came through the data loader");
            engine.getGroupletService().clearCache();
            context.remove(CTX_KEY_FLUSH_GROUPLETS_NEEDED);
        }
        
        if (context.get(CTX_KEY_FLUSH_LOADFILTERS_NEEDED) != null) {
            log.info("Load filters flushed because new filter config came through the data loader");
            engine.getLoadFilterService().clearCache();
            context.remove(CTX_KEY_FLUSH_LOADFILTERS_NEEDED);
        }
                
        if (context.get(CTX_KEY_FLUSH_CHANNELS_NEEDED) != null) {
            log.info("Channels flushed because new channels came through the data loader");
            engine.getConfigurationService().clearCache();
            context.remove(CTX_KEY_FLUSH_CHANNELS_NEEDED);
        }
        
        if (context.get(CTX_KEY_FLUSH_CONFLICTS_NEEDED) != null) {
            log.info("About to refresh the cache of conflict settings because new configuration came through the data loader");
            engine.getDataLoaderService().clearCache();
            context.remove(CTX_KEY_FLUSH_CONFLICTS_NEEDED);
        }

        if (context.get(CTX_KEY_FLUSH_PARAMETERS_NEEDED) != null) {
            log.info("About to refresh the cache of parameters because new configuration came through the data loader");
            parameterService.rereadParameters();
            context.remove(CTX_KEY_FLUSH_PARAMETERS_NEEDED);
        }

        if (context.get(CTX_KEY_FLUSH_NODE_SECURITY_NEEDED) != null) {
            log.info("About to refresh the cache of node security because new configuration came through the data loader");
            nodeService.flushNodeAuthorizedCache();
            context.remove(CTX_KEY_FLUSH_NODE_SECURITY_NEEDED);
        }

        if (context.get(CTX_KEY_FLUSH_NODE_NEEDED) != null) {
            log.info("About to refresh the cache of nodes because new configuration came through the data loader");
            nodeService.flushNodeCache();
            nodeService.flushNodeGroupCache();
            context.remove(CTX_KEY_FLUSH_NODE_NEEDED);
        }    

    }
}
