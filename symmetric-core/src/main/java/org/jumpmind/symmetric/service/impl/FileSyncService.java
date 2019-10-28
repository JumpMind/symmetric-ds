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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlReadCursor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.AbstractSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ContextConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.file.DirectorySnapshot;
import org.jumpmind.symmetric.file.FileConflictException;
import org.jumpmind.symmetric.file.FileSyncZipDataWriter;
import org.jumpmind.symmetric.file.FileTriggerFileModifiedListener;
import org.jumpmind.symmetric.file.FileTriggerFileModifiedListener.FileModifiedCallback;
import org.jumpmind.symmetric.file.FileTriggerTracker;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Data;
import org.jumpmind.symmetric.model.FileConflictStrategy;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.NoContentException;
import org.jumpmind.symmetric.transport.file.FileIncomingTransport;
import org.jumpmind.symmetric.transport.file.FileOutgoingTransport;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.ExceptionUtils;

import bsh.EvalError;
import bsh.Interpreter;
import bsh.TargetError;

public class FileSyncService extends AbstractOfflineDetectorService implements IFileSyncService,
INodeCommunicationExecutor {

    private ISymmetricEngine engine;
    
    private List<FileTriggerRouter> fileTriggerRoutersCache = new ArrayList<FileTriggerRouter>();
    private long fileTriggerRoutersCacheTime;
    private Object cacheLock = new Object();
    private Date lastUpdateTime;

    public FileSyncService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect(), engine.getExtensionService());
        this.engine = engine;
        setSqlMap(new FileSyncServiceSqlMap(platform, createSqlReplacementTokens()));
    }

    public boolean refreshFromDatabase() {
        Date date1 = sqlTemplate.queryForObject(getSql("selectMaxTriggerLastUpdateTime"), Date.class);
        Date date2 = sqlTemplate.queryForObject(getSql("selectMaxRouterLastUpdateTime"), Date.class);
        Date date3 = sqlTemplate.queryForObject(getSql("selectMaxFileTriggerRouterLastUpdateTime"), Date.class);
        Date date = maxDate(date1, date2, date3);
        
        if (date != null) {
            if (lastUpdateTime == null || lastUpdateTime.before(date)) {
                if (lastUpdateTime != null) {
                   log.info("Newer trigger router settings were detected");
                }
                lastUpdateTime = date;
                clearCache();
                return true;
            }
        }
        return false;
    }

    public void trackChanges(boolean force) {
        if (force || engine.getClusterService().lock(ClusterConstants.FILE_SYNC_TRACKER)) {
            try {
                log.debug("Attempting to get exclusive lock for file sync track changes");
                if (engine.getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE,
                        getParameterService().getLong(ParameterConstants.FILE_SYNC_LOCK_WAIT_MS))) {
                    try {
                        log.debug("Tracking changes for file sync");
                        Node local = engine.getNodeService().findIdentity();
                        if (local == null) {
                            log.warn("Not running file sync trackChanges because the local node is not available yet.  It may not be registered yet.");
                            return;
                        }
                        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                                new ProcessInfoKey(local.getNodeId(), null, ProcessType.FILE_SYNC_TRACKER));
                        boolean useCrc = engine.getParameterService().is(ParameterConstants.FILE_SYNC_USE_CRC);

                        if (engine.getParameterService().is(ParameterConstants.FILE_SYNC_FAST_SCAN)) {
                            trackChangesFastScan(processInfo, useCrc);
                        } else {
                            trackChanges(processInfo, useCrc);
                        }
                        if (engine.getParameterService().is(ParameterConstants.FILE_SYNC_PREVENT_PING_BACK)) {
                            deleteFromFileIncoming();
                        }
                        processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
                    } finally {
                        log.debug("Done tracking changes for file sync");
                        engine.getClusterService().unlock(ClusterConstants.FILE_SYNC_SHARED,
                                ClusterConstants.TYPE_EXCLUSIVE);
                    }
                } else {
                    log.warn("Did not run the track file sync changes process because it was shared locked");
                }
            } finally {
                if (!force) {
                    engine.getClusterService().unlock(ClusterConstants.FILE_SYNC_TRACKER);
                }
            }
        } else {
            log.debug("Did not run the track file sync changes process because it was cluster locked");
        }        
    }

    protected void trackChanges(ProcessInfo processInfo, boolean useCrc) {
    	long ctxTime = engine.getContextService().getLong(ContextConstants.FILE_SYNC_FAST_SCAN_TRACK_TIME);
        Date ctxDate = new Date(ctxTime);
        if (ctxTime == 0) {
            ctxDate = null;
        }
        Date currentDate = new Date();
        
        List<FileTriggerRouter> fileTriggerRouters = getFileTriggerRoutersForCurrentNode(false);
        for (FileTriggerRouter fileTriggerRouter : fileTriggerRouters) {
            if (fileTriggerRouter.isEnabled()) {
                try {
                	FileTrigger fileTrigger = fileTriggerRouter.getFileTrigger();
                	boolean ignoreFiles = shouldIgnoreInitialFiles(fileTriggerRouter, fileTrigger, ctxDate);
                    FileTriggerTracker tracker = new FileTriggerTracker(fileTriggerRouter, getDirectorySnapshot(fileTriggerRouter), 
                            processInfo, useCrc, engine);
                    DirectorySnapshot dirSnapshot = tracker.trackChanges();
                    saveDirectorySnapshot(fileTriggerRouter, dirSnapshot,ignoreFiles);
                    engine.getContextService().save(ContextConstants.FILE_SYNC_FAST_SCAN_TRACK_TIME, String.valueOf(currentDate.getTime()));
                } catch (Exception ex) {
                    log.error("Failed to track changes for file trigger router: "
                            + fileTriggerRouter.getFileTrigger().getTriggerId()
                            + "::" + fileTriggerRouter.getRouter().getRouterId(), ex);
                }
            }
        }
    }

    protected void trackChangesFastScan(ProcessInfo processInfo, boolean useCrc) {
        long ctxTime = engine.getContextService().getLong(ContextConstants.FILE_SYNC_FAST_SCAN_TRACK_TIME);
        Date ctxDate = new Date(ctxTime);
        if (ctxTime == 0) {
            ctxDate = null;
        }
        Date currentDate = new Date();

        int maxRowsBeforeCommit = engine.getParameterService().getInt(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT);

        try {
            List<FileTriggerRouter> fileTriggerRouters = getFileTriggerRoutersForCurrentNode(false);
            for (final FileTriggerRouter fileTriggerRouter : fileTriggerRouters) {
                if (fileTriggerRouter.isEnabled()) {
                	FileTrigger fileTrigger = fileTriggerRouter.getFileTrigger();
                	boolean ignoreFiles = shouldIgnoreInitialFiles(fileTriggerRouter, fileTrigger, ctxDate);
                    FileAlterationObserver observer = new FileAlterationObserver(fileTriggerRouter.getFileTrigger().getBaseDir(),
                            fileTriggerRouter.getFileTrigger().createIOFileFilter());
                    FileTriggerFileModifiedListener listener = new FileTriggerFileModifiedListener(fileTriggerRouter, ctxDate,
                            currentDate, processInfo, useCrc, new FileModifiedCallback(maxRowsBeforeCommit) {
                        public void commit(DirectorySnapshot dirSnapshot) {
                            saveDirectorySnapshot(fileTriggerRouter, dirSnapshot, ignoreFiles);
                        }

                        public DirectorySnapshot getLastDirectorySnapshot(String relativeDir) {
                            return getDirectorySnapshot(fileTriggerRouter, relativeDir);
                        }
                    }, engine);
                    observer.addListener(listener);
                    observer.checkAndNotify();
                    engine.getContextService().save(ContextConstants.FILE_SYNC_FAST_SCAN_TRACK_TIME, String.valueOf(currentDate.getTime()));
                }
            }
        } catch (Exception ex) {
            log.error("Failed to track changes", ex);            
        }
    }
    
    protected boolean shouldIgnoreInitialFiles(FileTriggerRouter router, FileTrigger trigger, Date contextDate) {
    	if (!router.isInitialLoadEnabled()) {
    		if (contextDate == null || router.getLastUpdateTime().after(contextDate) || trigger.getLastUpdateTime().after(contextDate)) {
        		return true;
        	}
    	}
    	return false;
    }

    protected long saveDirectorySnapshot(FileTriggerRouter fileTriggerRouter, DirectorySnapshot dirSnapshot, boolean shouldIgnore) {
        long totalBytes = 0;
        for (FileSnapshot fileSnapshot : dirSnapshot) {
            File file = fileTriggerRouter.getFileTrigger().createSourceFile(fileSnapshot);
            String filePath = file.getParentFile().getPath().replace('\\', '/');
            String fileName = file.getName();
            String nodeId = null;
            if (engine.getParameterService().is(ParameterConstants.FILE_SYNC_PREVENT_PING_BACK)) {
                nodeId = findSourceNodeIdFromFileIncoming(filePath,
                        fileName, fileSnapshot.getFileModifiedTime());
            }
            if (StringUtils.isNotBlank(nodeId)) {
                fileSnapshot.setLastUpdateBy(nodeId);
            } else {
                fileSnapshot.setLastUpdateBy(null);
            }
            log.debug("Captured change " + fileSnapshot);
            totalBytes += fileSnapshot.getFileSize();
        }
        save(dirSnapshot,shouldIgnore);
        return totalBytes;
    }

    protected String findSourceNodeIdFromFileIncoming(String filePath, String fileName,
            long lastUpdateDate) {
        return sqlTemplate.queryForString(getSql("findNodeIdFromFileIncoming"), filePath, fileName,
                lastUpdateDate);
    }

    protected void deleteFromFileIncoming() {
        sqlTemplate.update(getSql("deleteFileIncoming"));
    }

    public List<FileTrigger> getFileTriggers() {
        return sqlTemplate.query(getSql("selectFileTriggersSql"), new FileTriggerMapper());
    }

    public FileTrigger getFileTrigger(String triggerId) {
        return sqlTemplate.queryForObject(getSql("selectFileTriggersSql", "triggerIdWhere"),
                new FileTriggerMapper(), triggerId);
    }

    public List<FileTriggerRouter> getFileTriggerRoutersForCurrentNode(boolean refreshCache) {
        String myNodeGroupId = parameterService.getNodeGroupId();
        List<FileTriggerRouter> allValues = getFileTriggerRouters(refreshCache);
        List<FileTriggerRouter> currentValues = new ArrayList<FileTriggerRouter>();
        
        for(FileTriggerRouter ftr : allValues) {
            if(ftr.getRouter().getNodeGroupLink().getSourceNodeGroupId().equals(myNodeGroupId) && ftr.isEnabled()) {
                currentValues.add(ftr);
            }
        }
        return currentValues;
    }

    public List<FileTriggerRouter> getFileTriggerRouters(boolean refreshCache) {
        long fileTriggerRouterCacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS);
        List<FileTriggerRouter> currentValues = fileTriggerRoutersCache;
        
        if(currentValues == null || refreshCache || 
                System.currentTimeMillis() - this.fileTriggerRoutersCacheTime > fileTriggerRouterCacheTimeoutInMs) {
            synchronized (cacheLock) {
                List<FileTriggerRouter> newValues = sqlTemplate.query(getSql("selectFileTriggerRoutersSql"),
                        new FileTriggerRouterMapper());
                fileTriggerRoutersCache = newValues;
                currentValues = newValues;
                fileTriggerRoutersCacheTime = System.currentTimeMillis();
            }
        }
        
        return currentValues;
    }

    public FileTriggerRouter getFileTriggerRouter(String triggerId, String routerId, boolean refreshCache) {
        List<FileTriggerRouter> allValues = getFileTriggerRouters(refreshCache);
        
        for(FileTriggerRouter ftr: allValues) {
            if(ftr.getRouterId().equals(routerId) && ftr.getTriggerId().equals(triggerId)) {
                return ftr;
            }
        }
        return null;
    }
    
    
    public void clearCache() {
        synchronized (cacheLock) {
            this.fileTriggerRoutersCacheTime = 0;
        }
    }

    public void saveFileTrigger(FileTrigger fileTrigger) {
        fileTrigger.setLastUpdateTime(new Date());
        if (0 >= sqlTemplate.update(
                getSql("updateFileTriggerSql"),
                new Object[] { fileTrigger.getBaseDir(), fileTrigger.isRecurse() ? 1 : 0,
                        fileTrigger.getIncludesFiles(), fileTrigger.getExcludesFiles(),
                        fileTrigger.isSyncOnCreate() ? 1 : 0,
                                fileTrigger.isSyncOnModified() ? 1 : 0,
                                        fileTrigger.isSyncOnDelete() ? 1 : 0,
                                                fileTrigger.isSyncOnCtlFile() ? 1 : 0,
                                                        fileTrigger.isDeleteAfterSync() ? 1 : 0, fileTrigger.getBeforeCopyScript(),
                                                                fileTrigger.getAfterCopyScript(), fileTrigger.getLastUpdateBy(),
                                                                fileTrigger.getLastUpdateTime(), fileTrigger.getChannelId(),
                                                                fileTrigger.getReloadChannelId(), fileTrigger.getTriggerId() }, new int[] {
                                                                        Types.VARCHAR, Types.SMALLINT, 
                                                                        Types.VARCHAR, Types.VARCHAR,
                                                                        Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, 
                                                                        Types.SMALLINT, Types.VARCHAR,
                                                                        Types.VARCHAR, Types.VARCHAR,  
                                                                        Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR })) {
            fileTrigger.setCreateTime(fileTrigger.getLastUpdateTime());
            sqlTemplate.update(
                    getSql("insertFileTriggerSql"),
                    new Object[] { fileTrigger.getBaseDir(), fileTrigger.isRecurse() ? 1 : 0,
                            fileTrigger.getIncludesFiles(), fileTrigger.getExcludesFiles(),
                            fileTrigger.isSyncOnCreate() ? 1 : 0,
                                    fileTrigger.isSyncOnModified() ? 1 : 0,
                                            fileTrigger.isSyncOnDelete() ? 1 : 0,
                                                    fileTrigger.isSyncOnCtlFile() ? 1 : 0,
                                                            fileTrigger.isDeleteAfterSync() ? 1 : 0,
                                                                    fileTrigger.getBeforeCopyScript(), fileTrigger.getAfterCopyScript(),
                                                                    fileTrigger.getLastUpdateBy(), fileTrigger.getLastUpdateTime(),
                                                                    fileTrigger.getTriggerId(), fileTrigger.getCreateTime(),
                                                                    fileTrigger.getChannelId(), fileTrigger.getReloadChannelId() },
                    new int[] { Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                            Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                            Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR });
        }

    }

    public void saveFileTriggerRouter(FileTriggerRouter fileTriggerRouter) {
        fileTriggerRouter.setLastUpdateTime(new Date());
        if (0 >= sqlTemplate.update(
                getSql("updateFileTriggerRouterSql"),
                new Object[] { fileTriggerRouter.isEnabled() ? 1 : 0,
                        fileTriggerRouter.isInitialLoadEnabled() ? 1 : 0,
                                fileTriggerRouter.getTargetBaseDir(),
                                fileTriggerRouter.getConflictStrategyString(),
                                fileTriggerRouter.getLastUpdateBy(), fileTriggerRouter.getLastUpdateTime(),
                                fileTriggerRouter.getFileTrigger().getTriggerId(),
                                fileTriggerRouter.getRouter().getRouterId() }, new int[] { Types.SMALLINT,
                                        Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                                        Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR })) {
            fileTriggerRouter.setCreateTime(fileTriggerRouter.getLastUpdateTime());
            sqlTemplate.update(
                    getSql("insertFileTriggerRouterSql"),
                    new Object[] { fileTriggerRouter.isEnabled() ? 1 : 0,
                            fileTriggerRouter.isInitialLoadEnabled() ? 1 : 0,
                                    fileTriggerRouter.getTargetBaseDir(),
                                    fileTriggerRouter.getConflictStrategyString(),
                                    fileTriggerRouter.getCreateTime(), fileTriggerRouter.getLastUpdateBy(),
                                    fileTriggerRouter.getLastUpdateTime(),
                                    fileTriggerRouter.getFileTrigger().getTriggerId(),
                                    fileTriggerRouter.getRouter().getRouterId() }, new int[] {
                                            Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                                            Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                                            Types.VARCHAR });
        }
        clearCache();
    }

    public void deleteFileTriggerRouter(String triggerId, String routerId) {
        sqlTemplate.update(getSql("deleteFileTriggerRouterSql"), triggerId, routerId);
        clearCache();
    }

    public void deleteAllFileTriggerRouters() {
        sqlTemplate.update(getSql("deleteAllFileTriggerRoutersSql"));
        clearCache();
    }

    public void deleteFileTriggerRouter(FileTriggerRouter fileTriggerRouter) {
        sqlTemplate.update(getSql("deleteFileTriggerRouterSql"), (Object) fileTriggerRouter
                .getFileTrigger().getTriggerId(), fileTriggerRouter.getRouter().getRouterId());
        clearCache();
    }

    public void deleteFileTrigger(FileTrigger fileTrigger) {
        sqlTemplate.update(getSql("deleteFileTriggerSql"), (Object) fileTrigger.getTriggerId());
    }

    public DirectorySnapshot getDirectorySnapshot(FileTriggerRouter fileTriggerRouter) {
        return new DirectorySnapshot(fileTriggerRouter, sqlTemplate.query(
                getSql("selectFileSnapshotSql"), new FileSnapshotMapper(), fileTriggerRouter
                .getFileTrigger().getTriggerId(), fileTriggerRouter.getRouter()
                .getRouterId()));
    }

    public DirectorySnapshot getDirectorySnapshot(FileTriggerRouter fileTriggerRouter, String relativeDir) {
        return new DirectorySnapshot(fileTriggerRouter, sqlTemplate.query(
                getSql("selectFileSnapshotSql", "relativeDirWhere"), new FileSnapshotMapper(), fileTriggerRouter
                .getFileTrigger().getTriggerId(), fileTriggerRouter.getRouter()
                .getRouterId(), relativeDir));
    }

    public void save(List<FileSnapshot> changes, boolean shouldIgnore) {
        if (changes != null) {
            ISqlTransaction sqlTransaction = null;
            try {
                sqlTransaction = sqlTemplate.startSqlTransaction();
                if (shouldIgnore) {
                	engine.getSymmetricDialect().disableSyncTriggers(sqlTransaction, null);
                }
                for (FileSnapshot fileSnapshot : changes) {
                    save(sqlTransaction, fileSnapshot);
                }

                sqlTransaction.commit();
            } catch (Error ex) {
                if (sqlTransaction != null) {
                    sqlTransaction.rollback();
                }
                throw ex;
            } catch (RuntimeException ex) {
                if (sqlTransaction != null) {
                    sqlTransaction.rollback();
                }
                throw ex;
            } finally {
            	if (shouldIgnore && sqlTransaction != null) {
            		engine.getSymmetricDialect().enableSyncTriggers(sqlTransaction);
            	}
                close(sqlTransaction);
            }
        }
    }

    public void save(ISqlTransaction sqlTransaction, FileSnapshot snapshot) {
        snapshot.setLastUpdateTime(new Date());
        if (0 >= sqlTransaction.prepareAndExecute(
                getSql("updateFileSnapshotSql"),
                new Object[] { snapshot.getLastEventType().getCode(), snapshot.getCrc32Checksum(),
                        snapshot.getFileSize(), snapshot.getFileModifiedTime(),
                        snapshot.getLastUpdateTime(), snapshot.getLastUpdateBy(), snapshot.getChannelId(),
                        snapshot.getReloadChannelId(), 
                        snapshot.getTriggerId(), snapshot.getRouterId(), snapshot.getRelativeDir(),
                        snapshot.getFileName() }, new int[] { Types.VARCHAR, Types.NUMERIC,
                                Types.NUMERIC, Types.NUMERIC, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR })) {
            snapshot.setCreateTime(snapshot.getLastUpdateTime());
            sqlTransaction.prepareAndExecute(
                    getSql("insertFileSnapshotSql"),
                    new Object[] { snapshot.getLastEventType().getCode(),
                            snapshot.getCrc32Checksum(), snapshot.getFileSize(),
                            snapshot.getFileModifiedTime(), snapshot.getCreateTime(),
                            snapshot.getLastUpdateTime(), snapshot.getLastUpdateBy(), snapshot.getChannelId(),
                            snapshot.getReloadChannelId(), 
                            snapshot.getTriggerId(), snapshot.getRouterId(),
                            snapshot.getRelativeDir(), snapshot.getFileName() }, new int[] {
                                    Types.VARCHAR, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
                                    Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
        }
        // now that we have captured an update, delete the row for cleanup
        if (snapshot.getLastEventType() == LastEventType.DELETE) {
            sqlTransaction.prepareAndExecute(getSql("deleteFileSnapshotSql"), new Object[] {
                    snapshot.getTriggerId(), snapshot.getRouterId(), snapshot.getRelativeDir(),
                    snapshot.getFileName() }, new int[] { Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR });
        }

    }

    synchronized public RemoteNodeStatuses pullFilesFromNodes(boolean force) {
        CommunicationType communicationType = engine.getParameterService().is(ParameterConstants.NODE_OFFLINE) ? 
                CommunicationType.OFF_FSPULL : CommunicationType.FILE_PULL;
        
        return queueJob(force,
                parameterService.getLong(ParameterConstants.FILE_PULL_MINIMUM_PERIOD_MS, -1),
                ClusterConstants.FILE_SYNC_PULL, communicationType);
    }

    synchronized public RemoteNodeStatuses pushFilesToNodes(boolean force) {
        CommunicationType communicationType = engine.getParameterService().is(ParameterConstants.NODE_OFFLINE) ? 
                CommunicationType.OFF_FSPUSH : CommunicationType.FILE_PUSH;
        return queueJob(force,
                parameterService.getLong(ParameterConstants.FILE_PUSH_MINIMUM_PERIOD_MS, -1),
                ClusterConstants.FILE_SYNC_PUSH, communicationType);
    }

    @Override
    public Object[] getStagingPathComponents(OutgoingBatch fileSyncBatch) {
        StringBuilder zipName = new StringBuilder(32);
        zipName.append("filesync_").append(fileSyncBatch.getNodeBatchId()).append(".zip");
        return new String[] {Constants.STAGING_CATEGORY_OUTGOING, fileSyncBatch.getNodeId(), zipName.toString()};
    }

    public List<OutgoingBatch> sendFiles(ProcessInfo processInfo, Node targetNode,
            IOutgoingTransport outgoingTransport) {

        List<OutgoingBatch> batchesToProcess = getBatchesToProcess(targetNode);

        if (batchesToProcess.isEmpty()) {
            return batchesToProcess;
        }

        IStagingManager stagingManager = engine.getStagingManager();
        long maxBytesToSync = parameterService 
                .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);        

        List<OutgoingBatch> processedBatches = new ArrayList<OutgoingBatch>();

        OutgoingBatch currentBatch = null;
        
        IStagedResource stagedResource = null;
        IStagedResource previouslyStagedResource = null;
        
        FileSyncZipDataWriter dataWriter = null;
        try {
            long syncedBytes = 0;
            try {
                for (int i = 0; i < batchesToProcess.size(); i++) {
                    currentBatch = batchesToProcess.get(i);
                    
                    previouslyStagedResource = getStagedResource(currentBatch);
                    
                    if (isWaitForExtractionRequired(currentBatch, previouslyStagedResource)
                            || isFlushBatchesRequired(currentBatch, processedBatches, previouslyStagedResource)) {
                        // if we've already processed and staged some batches, send them now.  The 
                        // previously staged batch will have to wait for the next push/pull.
                        break;
                    }
                    
                    if (previouslyStagedResource != null) {
                        log.debug("Using existing extraction for file sync batch {}", currentBatch.getNodeBatchId());
                        stagedResource = previouslyStagedResource;
                    } else  {
                        if (dataWriter == null) {                        
                            stagedResource = stagingManager.create(
                                    Constants.STAGING_CATEGORY_OUTGOING, processInfo.getSourceNodeId(),
                                    targetNode.getNodeId(), "filesync.zip");                            
                            dataWriter = new FileSyncZipDataWriter(maxBytesToSync, this,
                                    engine.getNodeService(), stagedResource, engine.getExtensionService(), engine.getConfigurationService());
                        }
                        log.debug("Extracting batch {} for filesync.", currentBatch.getNodeBatchId());

                        ((DataExtractorService) engine.getDataExtractorService()).extractOutgoingBatch(
                                processInfo, targetNode, dataWriter, currentBatch, false, true,
                                DataExtractorService.ExtractMode.FOR_SYM_CLIENT, null);
                    }              
                    processedBatches.add(currentBatch);
                                        
                    syncedBytes += stagedResource.getSize();

                    processInfo.incrementBatchCount();
                    processInfo.setCurrentBatchId(currentBatch.getBatchId()); 
                    
                    log.debug("Processed file sync batch {}. syncedBytes={}, maxBytesToSync={}", currentBatch, syncedBytes, maxBytesToSync);

                    /*
                     * check to see if max bytes to sync has been reached and
                     * stop processing batches
                     */
                    if (previouslyStagedResource != null || dataWriter.readyToSend()) {
                        break;
                    }
                }
            } finally {  
                if (dataWriter != null) {
                    dataWriter.finish();
                }
            }

            processInfo.setStatus(ProcessInfo.ProcessStatus.TRANSFERRING);

            for (OutgoingBatch outgoingBatch : processedBatches) {
                outgoingBatch.setStatus(Status.SE);
            }
            engine.getOutgoingBatchService().updateOutgoingBatches(processedBatches);

            try {
                if (stagedResource != null && stagedResource.exists()) {
                    InputStream is = stagedResource.getInputStream();
                    try {
                        OutputStream os = outgoingTransport.openStream();
                        IOUtils.copy(is, os);
                        os.flush();
                    } catch (IOException e) {
                        throw new IoException(e);
                    }
                }

                for (int i = 0; i < batchesToProcess.size(); i++) {
                    batchesToProcess.get(i).setStatus(Status.LD);
                }
                engine.getOutgoingBatchService().updateOutgoingBatches(batchesToProcess);

            } finally {
                if (stagedResource != null) {
                    stagedResource.close();                    
                }
            }

        } catch (RuntimeException e) {
            if (stagedResource == previouslyStagedResource) { // on error, don't let the load extract be deleted.
                stagedResource = null;
            }
            
            if (currentBatch != null) {
                engine.getStatisticManager().incrementDataExtractedErrors(
                        currentBatch.getChannelId(), 1);
                currentBatch.setSqlMessage(ExceptionUtils.getRootMessage(e));
                currentBatch.revertStatsOnError();
                if (currentBatch.getStatus() != Status.IG) {
                    currentBatch.setStatus(Status.ER);
                }
                currentBatch.setErrorFlag(true);
                engine.getOutgoingBatchService().updateOutgoingBatch(currentBatch);

                if (isStreamClosedByClient(e)) {
                    log.warn(
                            "Failed to extract file sync batch {}.  The stream was closed by the client.  The error was: {}",
                            currentBatch, ExceptionUtils.getRootMessage(e));
                } else {
                    log.error("Failed to extract file sync batch " + currentBatch, e);
                }
            } else {
                log.error("Could not log the outgoing batch status because the batch was null", e);
            }

            throw e;
        } finally {
            if (stagedResource != null) {
                stagedResource.delete();
            }
        }

        return processedBatches;
    }

    private boolean isFlushBatchesRequired(OutgoingBatch currentBatch, List<OutgoingBatch> processedBatches, IStagedResource previouslyStagedResource) {
        // if we staged some on the fly and now come across a previously staged batch, need to flush the ones in flight.
        boolean isFlushBatchesRequred = previouslyStagedResource != null && !processedBatches.isEmpty();
        if (isFlushBatchesRequred) {
            log.info("Batch will be sent on the next sync. {} Need to flush newly staged file sync batches now.", currentBatch.getNodeBatchId());            
        }
        return isFlushBatchesRequred;    
    }

    private boolean isWaitForExtractionRequired(OutgoingBatch currentBatch, IStagedResource previouslyStagedResource) {
        Channel channel = this.engine.getConfigurationService().getChannel(currentBatch.getChannelId()); 
        
        if (previouslyStagedResource == null 
                && channel.isReloadFlag() 
                && parameterService.is(ParameterConstants.INITIAL_LOAD_USE_EXTRACT_JOB)) {
            
            if (currentBatch.getStatus() == OutgoingBatch.Status.RQ) {                
                // if this is a reload that isn't extracted yet, we need to defer to the extract job.
                log.info("Batch needs to be extracted by the extact job {}", currentBatch.getNodeBatchId());
            } else {
                // it's possible there was an error and staging was cleared, so we need to re-request extraction here.
                log.info("Batch has status of '{}' but is not extracted. Requesting re-extract for batch: {}", 
                        currentBatch.getStatus(), currentBatch.getNodeBatchId());
                engine.getDataExtractorService().resetExtractRequest(currentBatch);
            }
            
            return true;
        } else {            
            return false;
        }
    }

    protected List<OutgoingBatch> getBatchesToProcess(Node targetNode) {
        List<OutgoingBatch> batchesToProcess = new ArrayList<OutgoingBatch>();
        List<Channel> fileSyncChannels = engine.getConfigurationService().getFileSyncChannels();
        OutgoingBatches batches = engine.getOutgoingBatchService().getOutgoingBatches(
                targetNode.getNodeId(), false);
        for (Channel channel : fileSyncChannels) {
            batchesToProcess.addAll(batches.filterBatchesForChannel(channel));
        }
        return batchesToProcess;
    }

    public void acknowledgeFiles(OutgoingBatch outgoingBatch) {
        log.debug("Acknowledging file_sync outgoing batch-{}", outgoingBatch.getBatchId());
        List<File> filesToDelete = new ArrayList<File>();
        Table snapshotTable = platform.getTableFromCache(
                TableConstants.getTableName(tablePrefix, TableConstants.SYM_FILE_SNAPSHOT), false);
        ISqlReadCursor<Data> cursor = engine.getDataService().selectDataFor(
                outgoingBatch.getBatchId(), outgoingBatch.getChannelId());
        Data data = cursor.next();
        while (data != null) {
            if (data.getDataEventType() == DataEventType.INSERT || data.getDataEventType() == DataEventType.UPDATE) {
                Map<String, String> columnData = data.toColumnNameValuePairs(
                        snapshotTable.getColumnNames(), CsvData.ROW_DATA);

                FileSnapshot fileSnapshot = new FileSnapshot();
                fileSnapshot.setTriggerId(columnData.get("TRIGGER_ID"));
                fileSnapshot.setRouterId(columnData.get("ROUTER_ID"));
                fileSnapshot.setFileModifiedTime(Long.parseLong(columnData
                        .get("FILE_MODIFIED_TIME")));
                fileSnapshot.setFileName(columnData.get("FILE_NAME"));
                fileSnapshot.setRelativeDir(columnData.get("RELATIVE_DIR"));
                fileSnapshot.setLastEventType(LastEventType.fromCode(columnData
                        .get("LAST_EVENT_TYPE")));

                FileTriggerRouter triggerRouter = this.getFileTriggerRouter(
                        fileSnapshot.getTriggerId(), fileSnapshot.getRouterId(), false);
                if (triggerRouter != null) {
                    FileTrigger fileTrigger = triggerRouter.getFileTrigger();

                    if (fileTrigger.isDeleteAfterSync()) {
                        File file = fileTrigger.createSourceFile(fileSnapshot);
                        if (!file.isDirectory()) {
                            filesToDelete.add(file);
                            if (fileTrigger.isSyncOnCtlFile()) {
                                filesToDelete.add(this.getControleFile(file));
                            }
                        }
                    }
                    else if (parameterService.is(ParameterConstants.FILE_SYNC_DELETE_CTL_FILE_AFTER_SYNC, false)) {
                        File file = fileTrigger.createSourceFile(fileSnapshot);
                        if (!file.isDirectory()) {
                            if (fileTrigger.isSyncOnCtlFile()) {
                                filesToDelete.add(this.getControleFile(file));
                            }
                        }
                    }
                }
            }
            data = cursor.next();
        }

        if (cursor != null) {
            cursor.close();
            cursor = null;
        }

        if (filesToDelete != null && filesToDelete.size() > 0) {
            for (File file : filesToDelete) {
                if (file != null && file.exists()) {
                    log.debug("Deleting the '{}' file", file.getAbsolutePath());
                    boolean deleted = FileUtils.deleteQuietly(file);
                    if (!deleted) {
                        log.warn("Failed to 'delete on sync' the {} file", file.getAbsolutePath());
                    }
                }
                file = null;
            }
            filesToDelete = null;
        }
    }

    public void loadFilesFromPush(String nodeId, InputStream in, OutputStream out) {
        INodeService nodeService = engine.getNodeService();
        Node local = nodeService.findIdentity();
        Node sourceNode = nodeService.findNode(nodeId, true);
        if (local != null && sourceNode != null) {
            ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                    new ProcessInfoKey(nodeId, local.getNodeId(),
                            ProcessType.FILE_SYNC_PUSH_HANDLER));
            try {
                List<IncomingBatch> list = processZip(in, nodeId, processInfo);
                NodeSecurity security = nodeService.findNodeSecurity(local.getNodeId(), true);
                processInfo.setStatus(ProcessInfo.ProcessStatus.ACKING);
                engine.getTransportManager().writeAcknowledgement(out, sourceNode, list, local,
                        security != null ? security.getNodePassword() : null);
                processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
            } catch (Throwable e) {
                processInfo.setStatus(ProcessInfo.ProcessStatus.ERROR);
                if (e instanceof IOException) {
                    throw new IoException((IOException) e);
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        } else {
            throw new SymmetricException("Could not load data because the node is not registered");
        }
    }

    protected IStagedResource getStagedResource(OutgoingBatch currentBatch) {
        IStagedResource stagedResource = engine.getStagingManager().find(getStagingPathComponents(currentBatch));
        if (stagedResource != null && stagedResource.getState() == State.DONE) {
            return stagedResource;
        } else {
            return null;
        }
    }

    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        Node identity = engine.getNodeService().findIdentity();
        if (identity != null) {
            NodeSecurity security = engine.getNodeService().findNodeSecurity(identity.getNodeId(), true);
            if (security != null) {
                if (nodeCommunication.getCommunicationType() == CommunicationType.FILE_PULL
                        || nodeCommunication.getCommunicationType() == CommunicationType.OFF_FSPULL) {
                    pullFilesFromNode(nodeCommunication, status, identity, security);
                } else if (nodeCommunication.getCommunicationType() == CommunicationType.FILE_PUSH
                        || nodeCommunication.getCommunicationType() == CommunicationType.OFF_FSPUSH) {
                    pushFilesToNode(nodeCommunication, status, identity, security);
                }
            }
        }
    }

    protected void pushFilesToNode(NodeCommunication nodeCommunication, RemoteNodeStatus status,
            Node identity, NodeSecurity security) {
        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(nodeCommunication.getNodeId(), identity.getNodeId(),
                        ProcessType.FILE_SYNC_PUSH_JOB));
        IOutgoingWithResponseTransport transport = null;
        ITransportManager transportManager = null;
        try {
            
            if (!engine.getParameterService().is(ParameterConstants.NODE_OFFLINE)) {
                transportManager = engine.getTransportManager();
                transport = transportManager.getFilePushTransport(
                        nodeCommunication.getNode(), identity, security.getNodePassword(),
                        parameterService.getRegistrationUrl());
            } else {
                transportManager = ((AbstractSymmetricEngine)engine).getOfflineTransportManager();
                transport = transportManager.getFilePushTransport(
                        nodeCommunication.getNode(), identity, security.getNodePassword(),
                        parameterService.getRegistrationUrl());                
            }
            
            List<OutgoingBatch> batches = sendFiles(processInfo, nodeCommunication.getNode(),
                    transport);
            if (batches.size() > 0) {
                if (transport instanceof FileOutgoingTransport) {
                    ((FileOutgoingTransport) transport).setProcessedBatches(batches);
                }
                List<BatchAck> batchAcks = readAcks(batches, transport,
                        transportManager, engine.getAcknowledgeService(), null);
                status.updateOutgoingStatus(batches, batchAcks);
            }
            if (!status.failed() && batches.size() > 0) {
                log.info("Pushed files to {}. {} files and {} batches were processed",
                        new Object[] { nodeCommunication.getNodeId(), status.getDataProcessed(),
                                status.getBatchesProcessed()});
            } else if (status.failed()) {
                log.info("There was a failure while pushing files to {}. {} files and {} batches were processed",
                        new Object[] { nodeCommunication.getNodeId(), status.getDataProcessed(),
                                status.getBatchesProcessed()});                        
            }
        } catch (Exception e) {
            fireOffline(e, nodeCommunication.getNode(), status);
        } finally {
            if (processInfo.getStatus() != ProcessInfo.ProcessStatus.ERROR) {
                processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
            }
            if (transport != null) {
                transport.close();
                if (transport instanceof FileOutgoingTransport) {
                    ((FileOutgoingTransport) transport).complete(processInfo.getStatus() == ProcessInfo.ProcessStatus.OK);
                }
            }
        }
    }

    protected List<IncomingBatch> processZip(InputStream is, String sourceNodeId,
            ProcessInfo processInfo) throws IOException {
        File unzipDir = new File(parameterService.getTempDirectory(), String.format(
                "filesync_incoming/%s/%s", engine.getNodeService().findIdentityNodeId(),
                sourceNodeId));
        FileUtils.deleteDirectory(unzipDir);
        unzipDir.mkdirs();

        try {
            AppUtils.unzip(is, unzipDir);
        } catch (IoException ex) {
            if (ex.toString().contains("EOFException")) { // This happens on Android, when there is an empty zip.
                //log.debug("Caught exception while unzipping.", ex);
            } else {
                throw ex;
            }
        }

        Set<Long> batchIds = new TreeSet<Long>();
        String[] files = unzipDir.list(DirectoryFileFilter.INSTANCE);

        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                try {
                    batchIds.add(Long.parseLong(files[i]));
                } catch (NumberFormatException e) {
                    log.error(
                            "Unexpected directory name.  Expected a number representing a batch id.  Instead the directory was named '{}'",
                            files[i]);
                }
            }
        }

        List<IncomingBatch> batchesProcessed = new ArrayList<IncomingBatch>();

        IIncomingBatchService incomingBatchService = engine.getIncomingBatchService();

        processInfo.setStatus(ProcessInfo.ProcessStatus.LOADING);
        for (Long batchId : batchIds) {
            processInfo.setCurrentBatchId(batchId);
            processInfo.incrementBatchCount();
            File batchDir = new File(unzipDir, Long.toString(batchId));

            IncomingBatch incomingBatch = new IncomingBatch();

            File batchInfo = new File(batchDir, "batch-info.txt");
            if (batchInfo.exists()) {
                List<String> info = FileUtils.readLines(batchInfo);
                if (info != null && info.size() > 0) {
                    incomingBatch.setChannelId(info.get(0).trim());
                } else {
                    incomingBatch.setChannelId(Constants.CHANNEL_FILESYNC);
                }
            } else {
                incomingBatch.setChannelId(Constants.CHANNEL_FILESYNC);
            }

            incomingBatch.setBatchId(batchId);
            incomingBatch.setStatus(IncomingBatch.Status.LD);
            incomingBatch.setNodeId(sourceNodeId);
            incomingBatch.setByteCount(FileUtils.sizeOfDirectory(batchDir));
            batchesProcessed.add(incomingBatch);
            if (incomingBatchService.acquireIncomingBatch(incomingBatch)) {
                File syncScript = new File(batchDir, "sync.bsh");
                if (syncScript.exists()) {
                    String script = FileUtils.readFileToString(syncScript);
                    Interpreter interpreter = new Interpreter();
                    boolean isLocked = false;
                    try {
                        setInterpreterVariables(engine, sourceNodeId, batchDir, interpreter);

                        long waitMillis = getParameterService().getLong(
                                ParameterConstants.FILE_SYNC_LOCK_WAIT_MS);
                        log.debug("The {} node is attempting to get shared lock for to update incoming status", sourceNodeId);
                        isLocked = engine.getClusterService().lock(
                                ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED,
                                waitMillis);
                        if (isLocked) {
                            log.debug("The {} node got a shared file sync lock", sourceNodeId);
                            @SuppressWarnings("unchecked")
                            Map<String, String> filesToEventType = (Map<String, String>) interpreter
                            .eval(script);
                            if (engine.getParameterService().is(ParameterConstants.FILE_SYNC_PREVENT_PING_BACK)) {
                                updateFileIncoming(sourceNodeId, filesToEventType);
                            }
                            incomingBatch
                            .setLoadRowCount(filesToEventType != null ? filesToEventType
                                    .size() : 0);
                        } else {
                            throw new RuntimeException(
                                    "Could not obtain file sync shared lock within " + waitMillis
                                    + " millis");
                        }
                        incomingBatch.setStatus(IncomingBatch.Status.OK);
                        if (incomingBatchService.isRecordOkBatchesEnabled()) {
                            incomingBatchService.updateIncomingBatch(incomingBatch);
                        } else if (incomingBatch.isRetry()) {
                            incomingBatchService.deleteIncomingBatch(incomingBatch);
                        }
                    } catch (Throwable ex) {
                        if (ex instanceof TargetError) {
                            Throwable target = ((TargetError) ex).getTarget();
                            if (target != null) {
                                ex = target;
                            }
                        }
                        
                        String nodeIdBatchId = sourceNodeId + "-" + batchId;

                        if (ex instanceof EvalError) {
                            log.error("Failed to evalulate the script as part of file sync batch " + nodeIdBatchId + "\n" + script + "\n", ex);
                        } else if (ex instanceof FileConflictException) {
                            log.error(ex.getMessage() + ".  Failed to process file sync batch "
                                    + nodeIdBatchId);
                        } else {
                            log.error("Failed to process file sync for  batch " + nodeIdBatchId, ex);
                        }

                        incomingBatch.setErrorFlag(true);
                        incomingBatch.setStatus(IncomingBatch.Status.ER);
                        incomingBatch.setSqlMessage(ex.getMessage());
                        if (incomingBatchService.isRecordOkBatchesEnabled()
                                || incomingBatch.isRetry()) {
                            incomingBatchService.updateIncomingBatch(incomingBatch);
                        } else {
                            incomingBatchService.insertIncomingBatch(incomingBatch);
                        }
                        processInfo.setStatus(ProcessInfo.ProcessStatus.ERROR);
                        break;
                    } finally {
                        log.debug("The {} node is done processing file sync files", sourceNodeId);
                        if (isLocked) {
                            engine.getClusterService().unlock(ClusterConstants.FILE_SYNC_SHARED,
                                    ClusterConstants.TYPE_SHARED);
                        }
                    }
                } else {
                    log.error("Could not find the sync.bsh script for batch {}", batchId);
                }
            }

        }

        return batchesProcessed;
    }

    protected void setInterpreterVariables(ISymmetricEngine engine, String sourceNodeId, File batchDir, Interpreter interpreter) throws EvalError {
        interpreter.set("log", log);
        interpreter.set("batchDir", batchDir.getAbsolutePath().replace('\\', '/'));
        interpreter.set("engine", engine);
        interpreter.set("sourceNodeId", sourceNodeId);
    }

    protected void updateFileIncoming(String nodeId, Map<String, String> filesToEventType) {
        Set<String> filePaths = filesToEventType.keySet();
        for (String filePath : filePaths) {
            String eventType = filesToEventType.get(filePath);
            File file = new File(filePath);
            String fileName = file.getName();
            String dirName = file.getParentFile().getPath().replace('\\', '/');
            long lastUpdateTime = file.lastModified();
            int updateCount = sqlTemplate.update(getSql("updateFileIncoming"), nodeId,
                    lastUpdateTime, eventType, dirName, fileName);
            if (updateCount <= 0) {
                sqlTemplate.update(getSql("insertFileIncoming"), nodeId, lastUpdateTime, eventType,
                        dirName, fileName);
            }
        }
    }

    protected void pullFilesFromNode(NodeCommunication nodeCommunication, RemoteNodeStatus status,
            Node identity, NodeSecurity security) {
        IIncomingTransport transport = null;
        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(nodeCommunication.getNodeId(), identity.getNodeId(),
                        ProcessType.FILE_SYNC_PULL_JOB));
        try {
            processInfo.setStatus(ProcessInfo.ProcessStatus.TRANSFERRING);
            ITransportManager transportManager;
            
            if (!engine.getParameterService().is(ParameterConstants.NODE_OFFLINE)) {
                transportManager = engine.getTransportManager();
                transport = transportManager.getFilePullTransport(
                        nodeCommunication.getNode(), identity, security.getNodePassword(), null,
                        parameterService.getRegistrationUrl());
            } else {
                transportManager = ((AbstractSymmetricEngine)engine).getOfflineTransportManager();
                transport = transportManager.getFilePullTransport(
                        nodeCommunication.getNode(), identity, security.getNodePassword(), null,
                        parameterService.getRegistrationUrl());                
            }

            List<IncomingBatch> batchesProcessed = processZip(transport.openStream(),
                    nodeCommunication.getNodeId(), processInfo);

            if (batchesProcessed.size() > 0) {
                processInfo.setStatus(ProcessInfo.ProcessStatus.ACKING);
                status.updateIncomingStatus(batchesProcessed);
                sendAck(nodeCommunication.getNode(), identity, security, batchesProcessed,
                        transportManager);
            }
            if (!status.failed() && batchesProcessed.size() > 0) {
                log.info("Pull files received from {}.  {} files and {} batches were processed",
                        new Object[] { nodeCommunication.getNodeId(), status.getDataProcessed(),
                                status.getBatchesProcessed() });
            } else if (status.failed()) {
                log.info("There was a failure while pulling files from {}.  {} files and {} batches were processed",
                        new Object[] { nodeCommunication.getNodeId(), status.getDataProcessed(),
                                status.getBatchesProcessed() });
            }
        } catch (NoContentException noContentEx) {
            log.debug("Server reported no batches. " + noContentEx);
        } catch (Exception e) {
            fireOffline(e, nodeCommunication.getNode(), status);
        } finally {
            if (transport != null) {
                transport.close();
                if (processInfo.getStatus() != ProcessInfo.ProcessStatus.ERROR) {
                    processInfo.setStatus(ProcessInfo.ProcessStatus.OK);
                }
                if (transport instanceof FileIncomingTransport) {                    
                    ((FileIncomingTransport) transport).complete(!status.failed());
                }
            }
        }
    }

    protected RemoteNodeStatuses queueJob(boolean force, long minimumPeriodMs, String clusterLock,
            CommunicationType type) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses(engine.getConfigurationService().getChannels(false));
        Node identity = engine.getNodeService().findIdentity();
        if (identity != null && identity.isSyncEnabled()) {
            if (force || !engine.getClusterService().isInfiniteLocked(clusterLock)) {

                INodeCommunicationService nodeCommunicationService = engine
                        .getNodeCommunicationService();
                List<NodeCommunication> nodes = nodeCommunicationService.list(type);
                int availableThreads = nodeCommunicationService.getAvailableThreads(type);
                for (NodeCommunication nodeCommunication : nodes) {
                    if (StringUtils.isNotBlank(nodeCommunication.getNode().getSyncUrl())
                            || !parameterService.isRegistrationServer()) {
                        boolean meetsMinimumTime = true;
                        if (minimumPeriodMs > 0
                                && nodeCommunication.getLastLockTime() != null
                                && (System.currentTimeMillis() - nodeCommunication
                                        .getLastLockTime().getTime()) < minimumPeriodMs) {
                            meetsMinimumTime = false;
                        }
                        if (availableThreads > 0 && meetsMinimumTime) {
                            if (nodeCommunicationService.execute(nodeCommunication, statuses, this)) {
                                availableThreads--;
                            }
                        }
                    } else {
                        log.warn(
                                "File sync cannot communicate with node '{}' in the group '{}'.  The sync url is blank",
                                nodeCommunication.getNode().getNodeId(), nodeCommunication
                                .getNode().getNodeGroupId());
                    }
                }
            } else {
                log.debug("Did not run the {} process because it has been stopped", type.name()
                        .toLowerCase());
            }
        }

        return statuses;
    }

    protected String getEffectiveBaseDir(String baseDir) {
        String effectiveBaseDir = baseDir == null ? null : baseDir.replace('\\', '/');
        return effectiveBaseDir;
    }

    @Override
    public File getControleFile(File file) {
        File ctlFile = new File(file.getAbsolutePath() + FileTrigger.FILE_CTL_EXTENSION);
        if (engine.getParameterService().is(ParameterConstants.FILE_SYNC_USE_CTL_AS_FILE_EXT, false)) {
            int extPosition = file.getAbsolutePath().lastIndexOf('.');
            ctlFile = new File(file.getAbsolutePath().substring(0,extPosition) + ".ctl");
        }
        return ctlFile;
    }

    class FileTriggerMapper implements ISqlRowMapper<FileTrigger> {
        public FileTrigger mapRow(Row rs) {
            FileTrigger fileTrigger = new FileTrigger();
            fileTrigger.setBaseDir(getEffectiveBaseDir(rs.getString("base_dir")));
            fileTrigger.setCreateTime(rs.getDateTime("create_time"));
            fileTrigger.setExcludesFiles(rs.getString("excludes_files"));
            fileTrigger.setIncludesFiles(rs.getString("includes_files"));
            fileTrigger.setLastUpdateBy(rs.getString("last_update_by"));
            fileTrigger.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileTrigger.setRecurse(rs.getBoolean("recurse"));
            fileTrigger.setSyncOnCreate(rs.getBoolean("sync_on_create"));
            fileTrigger.setSyncOnDelete(rs.getBoolean("sync_on_delete"));
            fileTrigger.setAfterCopyScript(rs.getString("after_copy_script"));
            fileTrigger.setBeforeCopyScript(rs.getString("before_copy_script"));
            fileTrigger.setSyncOnModified(rs.getBoolean("sync_on_modified"));
            fileTrigger.setSyncOnCtlFile(rs.getBoolean("sync_on_ctl_file"));
            fileTrigger.setDeleteAfterSync(rs.getBoolean("delete_after_sync"));
            fileTrigger.setTriggerId(rs.getString("trigger_id"));
            fileTrigger.setChannelId(rs.getString("channel_id"));
            fileTrigger.setReloadChannelId(rs.getString("reload_channel_id"));
            return fileTrigger;
        }
    }

    class FileTriggerRouterMapper implements ISqlRowMapper<FileTriggerRouter> {
        public FileTriggerRouter mapRow(Row rs) {
            FileTriggerRouter fileTriggerRouter = new FileTriggerRouter();
            String triggerId = rs.getString("trigger_id");
            FileTrigger fileTrigger = getFileTrigger(triggerId);
            fileTriggerRouter.setFileTrigger(fileTrigger);
            try {
                fileTriggerRouter.setConflictStrategy(FileConflictStrategy.valueOf(rs.getString(
                    "conflict_strategy").toUpperCase()));
            }
            catch (Exception e) {
            }
            fileTriggerRouter.setConflictStrategyString(rs.getString(
                            "conflict_strategy").toUpperCase());
            fileTriggerRouter.setCreateTime(rs.getDateTime("create_time"));
            fileTriggerRouter.setLastUpdateBy(rs.getString("last_update_by"));
            fileTriggerRouter.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileTriggerRouter.setEnabled(rs.getBoolean("enabled"));
            fileTriggerRouter.setInitialLoadEnabled(rs.getBoolean("initial_load_enabled"));
            fileTriggerRouter.setTargetBaseDir((rs.getString("target_base_dir") == null) ? null
                    : rs.getString("target_base_dir").replace('\\', '/'));
            fileTriggerRouter.setRouter(engine.getTriggerRouterService().getRouterById(
                    rs.getString("router_id")));
            return fileTriggerRouter;
        }
    }

    static class FileSnapshotMapper implements ISqlRowMapper<FileSnapshot> {
        public FileSnapshot mapRow(Row rs) {
            FileSnapshot fileSnapshot = new FileSnapshot();
            fileSnapshot.setCrc32Checksum(rs.getLong("crc32_checksum"));
            fileSnapshot.setCreateTime(rs.getDateTime("create_time"));
            fileSnapshot.setChannelId(rs.getString("channel_id"));
            fileSnapshot.setReloadChannelId(rs.getString("reload_channel_id"));
            fileSnapshot.setLastUpdateBy(rs.getString("last_update_by"));
            fileSnapshot.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileSnapshot.setFileModifiedTime(rs.getLong("file_modified_time"));
            fileSnapshot.setFileName(rs.getString("file_name"));
            fileSnapshot.setRelativeDir(rs.getString("relative_dir") == null ? null : rs.getString(
                    "relative_dir").replace('\\', '/'));
            fileSnapshot.setFileSize(rs.getLong("file_size"));
            fileSnapshot.setLastEventType(LastEventType.fromCode(rs.getString("last_event_type")));
            fileSnapshot.setTriggerId(rs.getString("trigger_id"));
            fileSnapshot.setRouterId(rs.getString("router_id"));
            return fileSnapshot;
        }
    }

	@Override
	public void save(List<FileSnapshot> changes) {
		// TODO Auto-generated method stub
	}
}
