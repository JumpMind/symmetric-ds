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
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.file.DirectorySnapshot;
import org.jumpmind.symmetric.file.FileSyncZipDataWriter;
import org.jumpmind.symmetric.file.FileTriggerTracker;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.BatchAck;
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
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
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
import org.jumpmind.util.AppUtils;

import bsh.Interpreter;
import bsh.TargetError;

public class FileSyncService extends AbstractOfflineDetectorService implements IFileSyncService,
        INodeCommunicationExecutor {

    private Object trackerLock = new Object();
    private ISymmetricEngine engine;

    // TODO cache trigger routers

    public FileSyncService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.engine = engine;
        setSqlMap(new FileSyncServiceSqlMap(platform, createSqlReplacementTokens()));
    }

    public void trackChanges(boolean force) {
        synchronized (trackerLock) {
            if (engine.getClusterService().lock(ClusterConstants.FILE_SYNC_TRACKER) || force) {
                try {
                    List<FileTriggerRouter> fileTriggerRouters = getFileTriggerRoutersForCurrentNode();
                    for (FileTriggerRouter fileTriggerRouter : fileTriggerRouters) {
                        if (fileTriggerRouter.isEnabled()) {
                            FileTriggerTracker tracker = new FileTriggerTracker(fileTriggerRouter,
                                    getDirectorySnapshot(fileTriggerRouter));
                            try {
                                DirectorySnapshot dirSnapshot = tracker.trackChanges();
                                for (FileSnapshot fileSnapshot : dirSnapshot) {
                                    File file = fileTriggerRouter.getFileTrigger()
                                            .createSourceFile(fileSnapshot);
                                    String filePath = file.getParentFile().getPath();
                                    String fileName = file.getName();
                                    String nodeId = findSourceNodeIdFromFileIncoming(filePath,
                                            fileName, fileSnapshot.getFileModifiedTime());
                                    if (StringUtils.isNotBlank(nodeId)) {
                                        fileSnapshot.setLastUpdateBy(nodeId);
                                    }
                                }
                                save(dirSnapshot);
                            } catch (Exception ex) {
                                log.error("Failed to track changes for file trigger router: "
                                        + fileTriggerRouter.getFileTrigger().getTriggerId() + "::"
                                        + fileTriggerRouter.getRouter().getRouterId(), ex);
                            }
                        }
                    }

                    deleteFromFileIncoming();
                } finally {
                    engine.getClusterService().unlock(ClusterConstants.FILE_SYNC_TRACKER);
                }
            } else {
                log.debug("Did not run the track file sync changes process because it was locked");
            }
        }
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

    public List<FileTriggerRouter> getFileTriggerRoutersForCurrentNode() {
        return sqlTemplate.query(
                getSql("selectFileTriggerRoutersSql", "fileTriggerRoutersForCurrentNodeWhere"),
                new FileTriggerRouterMapper(), parameterService.getNodeGroupId());
    }

    public List<FileTriggerRouter> getFileTriggerRouters() {
        return sqlTemplate.query(
                getSql("selectFileTriggerRoutersSql"), new FileTriggerRouterMapper());
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
                new Object[] { fileTrigger.getBaseDir(), fileTrigger.isRecurse() ? 1 : 0,
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
                    new Object[] { fileTrigger.getBaseDir(), fileTrigger.isRecurse() ? 1 : 0,
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
                getSql("updateFileTriggerRouterSql"),
                new Object[] { fileTriggerRouter.isEnabled() ? 1 : 0,
                        fileTriggerRouter.isInitialLoadEnabled() ? 1 : 0,
                        fileTriggerRouter.getTargetBaseDir(),
                        fileTriggerRouter.getConflictStrategy().name(),
                        fileTriggerRouter.getLastUpdateBy(),
                        fileTriggerRouter.getLastUpdateTime(),
                        fileTriggerRouter.getFileTrigger().getTriggerId(),
                        fileTriggerRouter.getRouter().getRouterId() },
                        new int[] { Types.SMALLINT,  Types.SMALLINT,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR })) {
            fileTriggerRouter.setCreateTime(fileTriggerRouter.getLastUpdateTime());
            sqlTemplate.update(
                    getSql("insertFileTriggerRouterSql"),
                    new Object[] { fileTriggerRouter.isEnabled() ? 1 : 0,
                            fileTriggerRouter.isInitialLoadEnabled() ? 1 : 0,
                            fileTriggerRouter.getTargetBaseDir(),
                            fileTriggerRouter.getConflictStrategy().name(),
                            fileTriggerRouter.getCreateTime(),
                            fileTriggerRouter.getLastUpdateBy(),
                            fileTriggerRouter.getLastUpdateTime(),
                            fileTriggerRouter.getFileTrigger().getTriggerId(),
                            fileTriggerRouter.getRouter().getRouterId() },
                            new int[] { Types.SMALLINT, Types.SMALLINT,
                            Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.VARCHAR,
                            Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR });
        }
    }

    public void deleteFileTriggerRouter(FileTriggerRouter fileTriggerRouter) {
        sqlTemplate.update(getSql("deleteFileTriggerRouterSql"), (Object) fileTriggerRouter.getFileTrigger()
                .getTriggerId(), fileTriggerRouter.getRouter().getRouterId());
    }

    public void deleteFileTrigger(FileTrigger fileTrigger) {
        sqlTemplate.update(getSql("deleteFileTriggerSql"), (Object) fileTrigger.getTriggerId());
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
                        snapshot.getTriggerId(), snapshot.getRouterId(), snapshot.getRelativeDir(),
                        snapshot.getFileName() }, new int[] { Types.VARCHAR, Types.NUMERIC,
                        Types.NUMERIC, Types.NUMERIC, Types.TIMESTAMP, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR })) {
            snapshot.setCreateTime(snapshot.getLastUpdateTime());
            sqlTransaction.prepareAndExecute(
                    getSql("insertFileSnapshotSql"),
                    new Object[] { snapshot.getLastEventType().getCode(),
                            snapshot.getCrc32Checksum(), snapshot.getFileSize(),
                            snapshot.getFileModifiedTime(), snapshot.getCreateTime(),
                            snapshot.getLastUpdateTime(), snapshot.getLastUpdateBy(),
                            snapshot.getTriggerId(), snapshot.getRouterId(),
                            snapshot.getRelativeDir(), snapshot.getFileName() }, new int[] {
                            Types.VARCHAR, Types.NUMERIC, Types.NUMERIC, Types.NUMERIC,
                            Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR,
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
        return queueJob(force,
                parameterService.getLong(ParameterConstants.FILE_PULL_MINIMUM_PERIOD_MS, -1),
                ClusterConstants.FILE_SYNC_PULL, CommunicationType.FILE_PULL);
    }

    synchronized public RemoteNodeStatuses pushFilesToNodes(boolean force) {
        return queueJob(force,
                parameterService.getLong(ParameterConstants.FILE_PUSH_MINIMUM_PERIOD_MS, -1),
                ClusterConstants.FILE_SYNC_PUSH, CommunicationType.FILE_PUSH);
    }

    public List<OutgoingBatch> sendFiles(ProcessInfo processInfo, Node targetNode,
            IOutgoingTransport outgoingTransport) {

        OutgoingBatches batches = engine.getOutgoingBatchService().getOutgoingBatches(
                targetNode.getNodeId(), false);
        List<OutgoingBatch> activeBatches = batches
                .filterBatchesForChannel(Constants.CHANNEL_FILESYNC);

        OutgoingBatch currentBatch = null;

        IStagingManager stagingManager = engine.getStagingManager();
        IStagedResource stagedResource = stagingManager.create(Constants.STAGING_CATEGORY_OUTGOING,
                processInfo.getSourceNodeId(), targetNode.getNodeId(), "filesync.zip");

        try {

            long maxBytesToSync = parameterService
                    .getLong(ParameterConstants.TRANSPORT_MAX_BYTES_TO_SYNC);

            FileSyncZipDataWriter dataWriter = new FileSyncZipDataWriter(maxBytesToSync, this, engine.getNodeService(),
                    stagedResource);
            try {
                for (int i = 0; i < activeBatches.size(); i++) {
                    currentBatch = activeBatches.get(i);
                    processInfo.incrementBatchCount();
                    processInfo.setCurrentBatchId(currentBatch.getBatchId());

                    engine.getDataExtractorService().extractOutgoingBatch(processInfo, targetNode,
                            dataWriter, currentBatch, false);

                    /*
                     * check to see if max bytes to sync has been reached and
                     * stop processing batches
                     */
                    if (dataWriter.readyToSend()) {
                        break;
                    }
                }
            } finally {
                dataWriter.finish();
            }

            processInfo.setStatus(ProcessInfo.Status.TRANSFERRING);

            for (int i = 0; i < activeBatches.size(); i++) {
                activeBatches.get(i).setStatus(Status.SE);
            }
            engine.getOutgoingBatchService().updateOutgoingBatches(activeBatches);

            try {
                if (stagedResource.exists()) {
                    InputStream is = stagedResource.getInputStream();
                    try {
                        OutputStream os = outgoingTransport.openStream();
                        IOUtils.copy(is, os);
                        os.flush();
                    } catch (IOException e) {
                        throw new IoException(e);
                    }
                }

                for (int i = 0; i < activeBatches.size(); i++) {
                    activeBatches.get(i).setStatus(Status.LD);
                }
                engine.getOutgoingBatchService().updateOutgoingBatches(activeBatches);

            } finally {
                stagedResource.close();
            }

            return activeBatches;

        } catch (RuntimeException e) {
            if (currentBatch != null) {
                engine.getStatisticManager().incrementDataExtractedErrors(
                        currentBatch.getChannelId(), 1);
                currentBatch.setSqlMessage(getRootMessage(e));
                currentBatch.revertStatsOnError();
                if (currentBatch.getStatus() != Status.IG) {
                    currentBatch.setStatus(Status.ER);
                }
                currentBatch.setErrorFlag(true);
                engine.getOutgoingBatchService().updateOutgoingBatch(currentBatch);

                if (isStreamClosedByClient(e)) {
                    log.warn(
                            "Failed to extract batch {}.  The stream was closed by the client.  The error was: {}",
                            currentBatch, getRootMessage(e));
                } else {
                    log.error("Failed to extract batch {}", currentBatch, e);
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
    }

    public void loadFilesFromPush(String nodeId, InputStream in, OutputStream out) {
        INodeService nodeService = engine.getNodeService();
        Node local = nodeService.findIdentity();
        Node sourceNode = nodeService.findNode(nodeId);
        if (local != null && sourceNode != null) {
            ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                    new ProcessInfoKey(nodeId, local.getNodeId(),
                            ProcessInfoKey.ProcessType.FILE_SYNC_PUSH_HANDLER));
            try {
                List<IncomingBatch> list = processZip(in, nodeId, processInfo);
                NodeSecurity security = nodeService.findNodeSecurity(local.getNodeId());
                processInfo.setStatus(ProcessInfo.Status.ACKING);
                engine.getTransportManager().writeAcknowledgement(out, sourceNode, list, local,
                        security != null ? security.getNodePassword() : null);
                processInfo.setStatus(ProcessInfo.Status.DONE);
            } catch (Throwable e) {
                processInfo.setStatus(ProcessInfo.Status.ERROR);
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

    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        Node identity = engine.getNodeService().findIdentity();
        if (identity != null) {
            NodeSecurity security = engine.getNodeService().findNodeSecurity(identity.getNodeId());
            if (security != null) {
                if (nodeCommunication.getCommunicationType() == CommunicationType.FILE_PULL) {
                    pullFilesFromNode(nodeCommunication, status, identity, security);
                } else if (nodeCommunication.getCommunicationType() == CommunicationType.FILE_PUSH) {
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
        try {
            transport = engine.getTransportManager().getFilePushTransport(
                    nodeCommunication.getNode(), identity, security.getNodePassword(),
                    parameterService.getRegistrationUrl());
            List<OutgoingBatch> batches = sendFiles(processInfo, nodeCommunication.getNode(),
                    transport);
            if (batches.size() > 0) {
                List<BatchAck> batchAcks = readAcks(batches, transport,
                        engine.getTransportManager(), engine.getAcknowledgeService());
                status.updateOutgoingStatus(batches, batchAcks);
            }
        } catch (IOException e) {
            throw new IoException(e);
        } finally {
            if (transport != null) {
                transport.close();
            }
            if (processInfo.getStatus() != ProcessInfo.Status.ERROR) {
                processInfo.setStatus(ProcessInfo.Status.DONE);
            }
        }
    }

    protected List<IncomingBatch> processZip(InputStream is, String sourceNodeId,
            ProcessInfo processInfo) throws IOException {
        File unzipDir = new File(parameterService.getTempDirectory(), String.format("filesync_incoming/%s/%s",
                engine.getNodeService().findIdentityNodeId(),
                sourceNodeId));
        FileUtils.deleteDirectory(unzipDir);
        unzipDir.mkdirs();

        AppUtils.unzip(is, unzipDir);

        Set<Long> batchIds = new TreeSet<Long>();
        String[] files = unzipDir.list(DirectoryFileFilter.INSTANCE);
        for (int i = 0; i < files.length; i++) {
            try {
                batchIds.add(Long.parseLong(files[i]));
            } catch (NumberFormatException e) {
                log.error(
                        "Unexpected directory name.  Expected a number representing a batch id.  Instead the directory was named '{}'",
                        files[i]);
            }
        }

        List<IncomingBatch> batchesProcessed = new ArrayList<IncomingBatch>();

        IIncomingBatchService incomingBatchService = engine.getIncomingBatchService();

        processInfo.setStatus(ProcessInfo.Status.LOADING);
        for (Long batchId : batchIds) {
            processInfo.setCurrentBatchId(batchId);
            processInfo.incrementBatchCount();
            File batchDir = new File(unzipDir, Long.toString(batchId));
            File syncScript = new File(batchDir, "sync.bsh");

            IncomingBatch incomingBatch = new IncomingBatch();
            incomingBatch.setChannelId(Constants.CHANNEL_FILESYNC);
            incomingBatch.setBatchId(batchId);
            incomingBatch.setStatus(IncomingBatch.Status.LD);
            incomingBatch.setNodeId(sourceNodeId);
            incomingBatch.setByteCount(FileUtils.sizeOfDirectory(batchDir));
            batchesProcessed.add(incomingBatch);
            if (incomingBatchService.acquireIncomingBatch(incomingBatch)) {
                if (syncScript.exists()) {
                    String script = FileUtils.readFileToString(syncScript);
                    Interpreter interpreter = new Interpreter();
                    try {
                        interpreter.set("log", log);
                        interpreter.set("batchDir", batchDir.getAbsolutePath());
                        interpreter.set("engine", engine);
                        interpreter.set("sourceNodeId", sourceNodeId);

                        synchronized (trackerLock) {
                            @SuppressWarnings("unchecked")
                            Map<String, String> filesToEventType = (Map<String, String>) interpreter
                                    .eval(script);
                            updateFileIncoming(sourceNodeId, filesToEventType);
                            incomingBatch
                                    .setStatementCount(filesToEventType != null ? filesToEventType
                                            .size() : 0);
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

                        log.error("Failed to process file sync batch " + batchId, ex);

                        incomingBatch.setErrorFlag(true);
                        incomingBatch.setStatus(IncomingBatch.Status.ER);
                        incomingBatch.setSqlMessage(ex.getMessage());
                        if (incomingBatchService.isRecordOkBatchesEnabled() || incomingBatch.isRetry()) {
                            incomingBatchService.updateIncomingBatch(incomingBatch);
                        } else {
                            incomingBatchService.insertIncomingBatch(incomingBatch);
                        }
                        processInfo.setStatus(ProcessInfo.Status.ERROR);
                        break;

                    }
                } else {
                    log.error("Could not find the sync.bsh script for batch {}", batchId);
                }
            }

        }

        return batchesProcessed;
    }

    protected void updateFileIncoming(String nodeId, Map<String, String> filesToEventType) {
        Set<String> filePaths = filesToEventType.keySet();
        for (String filePath : filePaths) {
            String eventType = filesToEventType.get(filePath);
            File file = new File(filePath);
            String fileName = file.getName();
            String dirName = file.getParentFile().getPath();
            Date lastUpdateTime = new Date(file.lastModified());
            int updateCount = sqlTemplate.update(getSql("updateFileIncoming"), nodeId,
                    lastUpdateTime, eventType, dirName, fileName);
            if (updateCount == 0) {
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
            processInfo.setStatus(ProcessInfo.Status.TRANSFERRING);

            transport = engine.getTransportManager().getFilePullTransport(
                    nodeCommunication.getNode(), identity, security.getNodePassword(), null,
                    parameterService.getRegistrationUrl());

            List<IncomingBatch> batchesProcessed = processZip(transport.openStream(),
                    nodeCommunication.getNodeId(), processInfo);

            if (batchesProcessed.size() > 0) {
                processInfo.setStatus(ProcessInfo.Status.ACKING);
                status.updateIncomingStatus(batchesProcessed);
                sendAck(nodeCommunication.getNode(), identity, security, batchesProcessed,
                        engine.getTransportManager());
            }

        } catch (IOException e) {
            fireOffline(e, nodeCommunication.getNode(), status);
            throw new IoException(e);
        } finally {
            if (transport != null) {
                transport.close();
            }

            if (processInfo.getStatus() != ProcessInfo.Status.ERROR) {
                processInfo.setStatus(ProcessInfo.Status.DONE);
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
                    if (StringUtils.isNotBlank(nodeCommunication.getNode().getSyncUrl())
                            || !parameterService.isRegistrationServer()) {
                        boolean meetsMinimumTime = true;
                        if (minimumPeriodMs > 0
                                && nodeCommunication.getLastLockTime() != null
                                && (System.currentTimeMillis() - nodeCommunication
                                        .getLastLockTime().getTime()) < minimumPeriodMs) {
                            meetsMinimumTime = false;
                        }
                        if (availableThreads > 0 && !nodeCommunication.isLocked()
                                && meetsMinimumTime) {
                            nodeCommunicationService.execute(nodeCommunication, statuses, this);
                            availableThreads--;
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

    class FileTriggerMapper implements ISqlRowMapper<FileTrigger> {
        public FileTrigger mapRow(Row rs) {
            FileTrigger fileTrigger = new FileTrigger();
            fileTrigger.setBaseDir(rs.getString("base_dir"));
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
            fileSnapshot.setFileModifiedTime(rs.getLong("file_modified_time"));
            fileSnapshot.setFileName(rs.getString("file_name"));
            fileSnapshot.setRelativeDir(rs.getString("relative_dir"));
            fileSnapshot.setFileSize(rs.getLong("file_size"));
            fileSnapshot.setLastEventType(LastEventType.fromCode(rs.getString("last_event_type")));
            fileSnapshot.setTriggerId(rs.getString("trigger_id"));
            fileSnapshot.setRouterId(rs.getString("router_id"));
            return fileSnapshot;
        }
    }

}
