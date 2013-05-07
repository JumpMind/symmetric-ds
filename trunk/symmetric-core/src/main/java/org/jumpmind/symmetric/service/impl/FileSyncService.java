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

import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.file.DirectorySnapshot;
import org.jumpmind.symmetric.file.FileSyncZipDataWriter;
import org.jumpmind.symmetric.file.FileTriggerTracker;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.FileConflictStrategy;
import org.jumpmind.symmetric.model.FileSnapshot;
import org.jumpmind.symmetric.model.FileSnapshot.LastEventType;
import org.jumpmind.symmetric.model.FileTrigger;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class FileSyncService extends AbstractService implements IFileSyncService,
        INodeCommunicationExecutor {

    private ISymmetricEngine engine;
    private Map<FileTriggerRouter, FileTriggerTracker> trackers = new HashMap<FileTriggerRouter, FileTriggerTracker>();

    // TODO cache

    public FileSyncService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.engine = engine;
        setSqlMap(new FileSyncServiceSqlMap(platform, createSqlReplacementTokens()));
    }

    public void trackChanges(boolean force) {
        if (force
                || !engine.getClusterService().isInfiniteLocked(ClusterConstants.FILE_SYNC_TRACKER)) {
            List<FileTriggerRouter> fileTriggerRouters = getFileTriggerRoutersForCurrentNode();
            for (FileTriggerRouter fileTriggerRouter : fileTriggerRouters) {
                FileTriggerTracker tracker = trackers.get(fileTriggerRouter);
                if (tracker == null) {
                    tracker = new FileTriggerTracker(fileTriggerRouter,
                            getDirectorySnapshot(fileTriggerRouter));
                    trackers.put(fileTriggerRouter, tracker);
                }
                try {
                    save(tracker.trackChanges());
                } catch (Exception ex) {
                    // TODO build mechanism to rollback snapshot if we fail to
                    // save to database
                    log.error("Failed to track changes for file trigger router: "
                            + fileTriggerRouter.getFileTrigger().getTriggerId() + "::"
                            + fileTriggerRouter.getRouter().getRouterId(), ex);
                }
            }
        } else {
            log.debug("Did not run the track file sync changes process because it has been stopped");
        }
    }

    public List<FileTrigger> getFileTriggers() {
        return sqlTemplate.query(getSql("selectFileTriggersSql"), new FileTriggerMapper());
    }

    public FileTrigger getFileTrigger(String triggerId) {
        return sqlTemplate.queryForObject(getSql("selectFileTriggersSql", "triggerIdWhere"),
                new FileTriggerMapper(), triggerId);
    }

    public List<FileTriggerRouter> getFileTriggerRoutersForCurrentNode() {
        return sqlTemplate.query(
                getSql("selectFileTriggerRoutersSql", "fileTriggerRoutersForCurrentNodeWhere"),
                new FileTriggerRouterMapper(), parameterService.getNodeGroupId());
    }

    public FileTriggerRouter getFileTriggerRouter(String triggerId, String routerId) {
        return sqlTemplate.queryForObject(
                getSql("selectFileTriggerRoutersSql", "whereTriggerRouterId"),
                new FileTriggerRouterMapper(), triggerId, routerId);
    }

    public void saveFileTrigger(FileTrigger fileTrigger) {
        fileTrigger.setLastUpdateTime(new Date());
        if (0 == sqlTemplate.update(
                getSql("updateFileTriggerSql"),
                new Object[] { fileTrigger.getBaseDir(), fileTrigger.isRecursive() ? 1 : 0,
                        fileTrigger.getIncludesFiles(), fileTrigger.getExcludesFiles(),
                        fileTrigger.isSyncOnCreate() ? 1 : 0,
                        fileTrigger.isSyncOnModified() ? 1 : 0,
                        fileTrigger.isSyncOnDelete() ? 1 : 0, fileTrigger.getBeforeCopyScript(),
                        fileTrigger.getAfterCopyScript(), fileTrigger.getLastUpdateBy(),
                        fileTrigger.getLastUpdateTime(), fileTrigger.getTriggerId() }, new int[] {
                        Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                        Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR })) {
            fileTrigger.setCreateTime(fileTrigger.getLastUpdateTime());
            sqlTemplate.update(getSql("insertFileTriggerSql"),
                    new Object[] { fileTrigger.getBaseDir(), fileTrigger.isRecursive() ? 1 : 0,
                            fileTrigger.getIncludesFiles(), fileTrigger.getExcludesFiles(),
                            fileTrigger.isSyncOnCreate() ? 1 : 0,
                            fileTrigger.isSyncOnModified() ? 1 : 0,
                            fileTrigger.isSyncOnDelete() ? 1 : 0,
                            fileTrigger.getBeforeCopyScript(), fileTrigger.getAfterCopyScript(),
                            fileTrigger.getLastUpdateBy(), fileTrigger.getLastUpdateTime(),
                            fileTrigger.getTriggerId(), fileTrigger.getCreateTime() }, new int[] {
                            Types.VARCHAR, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                            Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                            Types.TIMESTAMP });
        }

    }

    public void saveFileTriggerRouter(FileTriggerRouter fileTriggerRouter) {
        fileTriggerRouter.setLastUpdateTime(new Date());
        if (0 == sqlTemplate.update(
                getSql("updateFileTriggerSql"),
                new Object[] { fileTriggerRouter.isEnabled() ? 1 : 0,
                        fileTriggerRouter.isInitialLoadEnabled() ? 1 : 0,
                        fileTriggerRouter.getTargetBaseDir(),
                        fileTriggerRouter.getConflictStrategy().name(),
                        fileTriggerRouter.getLastUpdateBy(), fileTriggerRouter.getLastUpdateTime(),
                        fileTriggerRouter.getFileTrigger().getTriggerId(),
                        fileTriggerRouter.getRouter().getRouterId() }, new int[] { Types.SMALLINT,
                        Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR })) {
            fileTriggerRouter.setCreateTime(fileTriggerRouter.getLastUpdateTime());
            sqlTemplate.update(
                    getSql("insertFileTriggerSql"),
                    new Object[] { fileTriggerRouter.isEnabled() ? 1 : 0,
                            fileTriggerRouter.isInitialLoadEnabled() ? 1 : 0,
                            fileTriggerRouter.getTargetBaseDir(),
                            fileTriggerRouter.getConflictStrategy().name(),
                            fileTriggerRouter.getCreateTime(), fileTriggerRouter.getLastUpdateBy(),
                            fileTriggerRouter.getLastUpdateTime(),
                            fileTriggerRouter.getFileTrigger().getTriggerId(),
                            fileTriggerRouter.getRouter().getRouterId() }, new int[] {
                            Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP,
                            Types.VARCHAR, Types.VARCHAR });
        }
    }

    public List<FileTriggerRouter> getFileTriggerRouters(FileTrigger fileTrigger) {
        return sqlTemplate.query(getSql("selectFileTriggerRoutersSql", "whereTriggerIdSql"),
                new FileTriggerRouterMapper(), fileTrigger.getTriggerId());
    }

    public DirectorySnapshot getDirectorySnapshot(FileTriggerRouter fileTriggerRouter) {
        return new DirectorySnapshot(fileTriggerRouter, sqlTemplate.query(
                getSql("selectFileSnapshotSql"), new FileSnapshotMapper(), fileTriggerRouter
                        .getFileTrigger().getTriggerId(), fileTriggerRouter.getRouter()
                        .getRouterId()));
    }

    public void save(List<FileSnapshot> changes) {
        if (changes != null) {
            ISqlTransaction sqlTransaction = null;
            try {

                sqlTransaction = sqlTemplate.startSqlTransaction();
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
                close(sqlTransaction);
            }
        }
    }

    public void save(ISqlTransaction sqlTransaction, FileSnapshot snapshot) {
        snapshot.setLastUpdateTime(new Date());
        if (0 == sqlTransaction.prepareAndExecute(
                getSql("updateFileSnapshotSql"),
                new Object[] { snapshot.getLastEventType().getCode(), snapshot.getCrc32Checksum(),
                        snapshot.getFileSize(), snapshot.getFileModifiedTime(),
                        snapshot.getLastUpdateTime(), snapshot.getLastUpdateBy(),
                        snapshot.getTriggerId(), snapshot.getRouterId(), snapshot.getFilePath(),
                        snapshot.getFileName() }, new int[] { Types.VARCHAR, Types.NUMERIC,
                        Types.NUMERIC, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR })) {
            snapshot.setCreateTime(snapshot.getLastUpdateTime());
            sqlTransaction.prepareAndExecute(
                    getSql("insertFileSnapshotSql"),
                    new Object[] { snapshot.getLastEventType().getCode(),
                            snapshot.getCrc32Checksum(), snapshot.getFileSize(),
                            snapshot.getFileModifiedTime(), snapshot.getCreateTime(),
                            snapshot.getLastUpdateTime(), snapshot.getLastUpdateBy(),
                            snapshot.getTriggerId(), snapshot.getRouterId(),
                            snapshot.getFilePath(), snapshot.getFileName() }, new int[] {
                            Types.VARCHAR, Types.NUMERIC, Types.NUMERIC, Types.TIMESTAMP,
                            Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
        }

    }

    synchronized public RemoteNodeStatuses pullFilesFromNodes(boolean force) {
        return queueJob(force,
                parameterService.getLong(ParameterConstants.FILE_PULL_MINIMUM_PERIOD_MS, -1),
                ClusterConstants.FILE_SYNC_PULL, CommunicationType.FILE_PULL);
    }

    synchronized public RemoteNodeStatuses pushFilesToNodes(boolean force) {
        return queueJob(force,
                parameterService.getLong(ParameterConstants.FILE_PUSH_MINIMUM_PERIOD_MS, -1),
                ClusterConstants.FILE_SYNC_PUSH, CommunicationType.FILE_PUSH);
    }

    public void sendFiles(ProcessInfo processInfo, Node targetNode,
            IOutgoingTransport outgoingTransport) {
        OutgoingBatches batches = engine.getOutgoingBatchService().getOutgoingBatches(
                targetNode.getNodeId(), false);
        List<OutgoingBatch> activeBatches = batches
                .filterBatchesForChannel(Constants.CHANNEL_FILESYNC);
        OutgoingBatch currentBatch = null;
        try {

            long maxBytesToSync = parameterService
                    .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);

            IStagingManager stagingManager = engine.getStagingManager();
            IStagedResource stagedResource = stagingManager.create(
                    Constants.STAGING_CATEGORY_OUTGOING, targetNode.getNodeId(), "filesync.zip");

            FileSyncZipDataWriter dataWriter = new FileSyncZipDataWriter(maxBytesToSync, this,
                    stagedResource);
            for (int i = 0; i < activeBatches.size(); i++) {
                currentBatch = activeBatches.get(i);
                processInfo.incrementBatchCount();
                processInfo.setCurrentBatchId(currentBatch.getBatchId());

                engine.getDataExtractorService().extractOutgoingBatch(processInfo, targetNode,
                        dataWriter, currentBatch, false);

                // check to see if max bytes to sync has been reached and stop
                // processing batches
                if (dataWriter.readyToSend()) {
                    break;
                }
            }

        } catch (RuntimeException e) {
            SQLException se = unwrapSqlException(e);
            if (currentBatch != null) {
                engine.getStatisticManager().incrementDataExtractedErrors(
                        currentBatch.getChannelId(), 1);
                if (se != null) {
                    currentBatch.setSqlState(se.getSQLState());
                    currentBatch.setSqlCode(se.getErrorCode());
                    currentBatch.setSqlMessage(se.getMessage());
                } else {
                    currentBatch.setSqlMessage(getRootMessage(e));
                }
                currentBatch.revertStatsOnError();
                if (currentBatch.getStatus() != Status.IG) {
                    currentBatch.setStatus(Status.ER);
                }
                currentBatch.setErrorFlag(true);
                engine.getOutgoingBatchService().updateOutgoingBatch(currentBatch);

                if (isStreamClosedByClient(e)) {
                    log.warn(
                            "Failed to extract batch {}.  The stream was closed by the client.  There is a good chance that a previously sent batch errored out and the stream was closed.  The error was: {}",
                            currentBatch, getRootMessage(e));
                } else {
                    log.error("Failed to extract batch {}", currentBatch, e);
                }
            } else {
                log.error("Could not log the outgoing batch status because the batch was null", e);
            }
        }

    }

    protected RemoteNodeStatuses queueJob(boolean force, long minimumPeriodMs, String clusterLock,
            CommunicationType type) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses();
        Node identity = engine.getNodeService().findIdentity(false);
        if (identity != null && identity.isSyncEnabled()) {
            if (force || !engine.getClusterService().isInfiniteLocked(clusterLock)) {

                INodeCommunicationService nodeCommunicationService = engine
                        .getNodeCommunicationService();
                List<NodeCommunication> nodes = nodeCommunicationService.list(type);
                int availableThreads = nodeCommunicationService.getAvailableThreads(type);
                for (NodeCommunication nodeCommunication : nodes) {
                    boolean meetsMinimumTime = true;
                    if (minimumPeriodMs > 0
                            && nodeCommunication.getLastLockTime() != null
                            && (System.currentTimeMillis() - nodeCommunication.getLastLockTime()
                                    .getTime()) < minimumPeriodMs) {
                        meetsMinimumTime = false;
                    }
                    if (availableThreads > 0 && !nodeCommunication.isLocked() && meetsMinimumTime) {
                        nodeCommunicationService.execute(nodeCommunication, statuses, this);
                        availableThreads--;
                    }
                }
            } else {
                log.debug("Did not run the {} process because it has been stopped", type.name()
                        .toLowerCase());
            }
        }

        return statuses;
    }

    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
    }

    class FileTriggerMapper implements ISqlRowMapper<FileTrigger> {
        public FileTrigger mapRow(Row rs) {
            FileTrigger fileTrigger = new FileTrigger();
            fileTrigger.setBaseDir(rs.getString("base_dir"));
            fileTrigger.setCreateTime(rs.getDateTime("create_time"));
            fileTrigger.setExcludesFiles(rs.getString("excludes_files"));
            fileTrigger.setIncludesFiles(rs.getString("includes_files"));
            fileTrigger.setLastUpdateBy(rs.getString("last_update_by"));
            fileTrigger.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileTrigger.setRecursive(rs.getBoolean("recursive"));
            fileTrigger.setSyncOnCreate(rs.getBoolean("sync_on_create"));
            fileTrigger.setSyncOnDelete(rs.getBoolean("sync_on_delete"));
            fileTrigger.setAfterCopyScript(rs.getString("after_copy_script"));
            fileTrigger.setBeforeCopyScript(rs.getString("before_copy_script"));
            fileTrigger.setSyncOnModified(rs.getBoolean("sync_on_modified"));
            fileTrigger.setTriggerId(rs.getString("trigger_id"));
            return fileTrigger;
        }
    }

    class FileTriggerRouterMapper implements ISqlRowMapper<FileTriggerRouter> {
        public FileTriggerRouter mapRow(Row rs) {
            FileTriggerRouter fileTriggerRouter = new FileTriggerRouter();
            String triggerId = rs.getString("trigger_id");
            FileTrigger fileTrigger = getFileTrigger(triggerId);
            fileTriggerRouter.setFileTrigger(fileTrigger);
            fileTriggerRouter.setConflictStrategy(FileConflictStrategy.valueOf(rs.getString(
                    "conflict_strategy").toUpperCase()));
            fileTriggerRouter.setCreateTime(rs.getDateTime("create_time"));
            fileTriggerRouter.setLastUpdateBy(rs.getString("last_update_by"));
            fileTriggerRouter.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileTriggerRouter.setEnabled(rs.getBoolean("enabled"));
            fileTriggerRouter.setInitialLoadEnabled(rs.getBoolean("initial_load_enabled"));
            fileTriggerRouter.setTargetBaseDir(rs.getString("target_base_dir"));
            fileTriggerRouter.setRouter(engine.getTriggerRouterService().getRouterById(
                    rs.getString("router_id")));
            return fileTriggerRouter;
        }
    }

    class FileSnapshotMapper implements ISqlRowMapper<FileSnapshot> {
        public FileSnapshot mapRow(Row rs) {
            FileSnapshot fileSnapshot = new FileSnapshot();
            fileSnapshot.setCrc32Checksum(rs.getLong("crc32_checksum"));
            fileSnapshot.setCreateTime(rs.getDateTime("create_time"));
            fileSnapshot.setLastUpdateBy(rs.getString("last_update_by"));
            fileSnapshot.setLastUpdateTime(rs.getDateTime("last_update_time"));
            fileSnapshot.setFileModifiedTime(rs.getDateTime("file_modified_time"));
            fileSnapshot.setFileName(rs.getString("file_name"));
            fileSnapshot.setFilePath(rs.getString("file_path"));
            fileSnapshot.setFileSize(rs.getLong("file_size"));
            fileSnapshot.setLastEventType(LastEventType.fromCode(rs.getString("last_event_type")));
            fileSnapshot.setTriggerId(rs.getString("trigger_id"));
            fileSnapshot.setRouterId(rs.getString("router_id"));
            return fileSnapshot;
        }
    }

}
