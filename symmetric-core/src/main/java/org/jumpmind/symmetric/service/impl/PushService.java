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

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatchByNodeChannelCount;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.ChannelDisabledException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.IIncomingTransport;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.ServiceUnavailableException;

/**
 * @see IPushService
 */
public class PushService extends AbstractOfflineDetectorService implements IPushService {

    protected ISymmetricEngine engine;

    protected Executor nodeChannelExtractForPushWorker;

    protected Set<NodeChannel> pushWorkersWorking = new HashSet<NodeChannel>();

    protected Executor nodeChannelTransportForPushWorker;

    public PushService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect(), engine.getExtensionService());
        this.engine = engine;
    }

    public void start() {
        nodeChannelExtractForPushWorker = (ThreadPoolExecutor) Executors.newCachedThreadPool(new ThreadFactory() {
            final AtomicInteger threadNumber = new AtomicInteger(1);
            final String namePrefix = parameterService.getEngineName().toLowerCase() + "-extract-for-push-";

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(namePrefix + threadNumber.getAndIncrement());
                t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        });

        nodeChannelTransportForPushWorker = (ThreadPoolExecutor) Executors.newCachedThreadPool(new ThreadFactory() {
            final AtomicInteger threadNumber = new AtomicInteger(1);
            final String namePrefix = parameterService.getEngineName().toLowerCase() + "-push-";

            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName(namePrefix + threadNumber.getAndIncrement());
                t.setDaemon(false);
                if (t.getPriority() != Thread.NORM_PRIORITY) {
                    t.setPriority(Thread.NORM_PRIORITY);
                }
                return t;
            }
        });
    }

    public void stop() {
        log.info("The push service is shutting down");
        if (nodeChannelExtractForPushWorker != null && nodeChannelExtractForPushWorker instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) nodeChannelExtractForPushWorker).shutdown();
        }
        nodeChannelExtractForPushWorker = null;

        if (nodeChannelTransportForPushWorker != null && nodeChannelTransportForPushWorker instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) nodeChannelTransportForPushWorker).shutdown();
        }
        nodeChannelTransportForPushWorker = null;

    }

    synchronized public RemoteNodeStatuses push(boolean force) {
        IConfigurationService configurationService = engine.getConfigurationService();
        IOutgoingBatchService outgoingBatchService = engine.getOutgoingBatchService();
        INodeService nodeService = engine.getNodeService();
        IClusterService clusterService = engine.getClusterService();

        int availableThreadPairs = parameterService.getInt(ParameterConstants.PUSH_THREAD_COUNT_PER_SERVER);
        long minimumPeriodBetweenPushesMs = parameterService.getLong(ParameterConstants.PUSH_MINIMUM_PERIOD_MS, -1);

        RemoteNodeStatuses statuses = new RemoteNodeStatuses(configurationService.getChannels(false));

        Node identityNode = nodeService.findIdentity(false);
        if (identityNode != null && identityNode.isSyncEnabled()) {
            List<NodeHost> hosts = nodeService.findNodeHosts(identityNode.getNodeId());
            int clusterInstanceCount = hosts != null && hosts.size() > 0 ? hosts.size() : 1;
            NodeSecurity identitySecurity = nodeService.findNodeSecurity(identityNode.getNodeId());

            if (identitySecurity != null && (force || !clusterService.isInfiniteLocked(ClusterConstants.PUSH))) {
                Iterator<OutgoingBatchByNodeChannelCount> nodeChannels = outgoingBatchService.getOutgoingBatchByNodeChannelCount(
                        availableThreadPairs * clusterInstanceCount, NodeGroupLinkAction.P, true).iterator();

                // TODO check for availablilty by channel in overall threadpool
                // based on percentage
                while (nodeChannels.hasNext() && pushWorkersWorking.size() < availableThreadPairs) {
                    OutgoingBatchByNodeChannelCount batchCount = nodeChannels.next();
                    String nodeId = batchCount.getNodeId();
                    String channelId = batchCount.getChannelId();
                    Node remoteNode = nodeService.findNode(nodeId);
                    NodeChannel nodeChannel = configurationService.getNodeChannel(channelId, nodeId, false);

                    if (nodeChannel != null && !nodeChannel.isFileSyncFlag() && !pushWorkersWorking.contains(nodeChannel)) {
                        boolean meetsMinimumTime = true;
                        // TODO error backoff logic
                        if (minimumPeriodBetweenPushesMs > 0 && nodeChannel.getLastExtractTime() != null
                                && (System.currentTimeMillis() - nodeChannel.getLastExtractTime().getTime()) < minimumPeriodBetweenPushesMs) {
                            meetsMinimumTime = false;
                        }

                        if (meetsMinimumTime && clusterService.lockNodeChannel(ClusterConstants.PUSH, nodeId, channelId)) {
                            NodeChannelExtractForPushWorker worker = new NodeChannelExtractForPushWorker(remoteNode, identityNode,
                                    identitySecurity, nodeChannel, statuses.add(nodeId, channelId));
                            pushWorkersWorking.add(nodeChannel);
                            nodeChannelExtractForPushWorker.execute(worker);
                        }
                    }
                }
            }
        }
        return statuses;
    }

    class NodeChannelExtractForPushWorker implements Runnable {

        RemoteNodeStatus status;

        Node targetNode;

        Node identityNode;

        NodeSecurity identitySecurity;

        NodeChannel nodeChannel;

        public NodeChannelExtractForPushWorker(Node remoteNode, Node identityNode, NodeSecurity identitySecurity, NodeChannel nodeChannel,
                RemoteNodeStatus status) {
            this.nodeChannel = nodeChannel;
            this.status = status;
            this.identitySecurity = identitySecurity;
            this.identityNode = identityNode;
            this.targetNode = remoteNode;
        }

        @Override
        public void run() {
            log.info("Preparing to push for {}", nodeChannel);
            IDataExtractorService dataExtractorService = engine.getDataExtractorService();
            IOutgoingBatchService outgoingBatchService = engine.getOutgoingBatchService();
            IClusterService clusterService = engine.getClusterService();
            IStatisticManager statisticManager = engine.getStatisticManager();

            String channelId = nodeChannel.getChannelId();

            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(identitySecurity.getNodeId(), nodeChannel
                    .getNodeId(), ProcessType.EXTRACT_FOR_PUSH, channelId));

            Exception error = null;
            NodeChannelTransportForPushWorker pushWorker = null;
            try {
                List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatchesForNodeChannel(targetNode.getNodeId(), nodeChannel);
                if (batches.size() > 0 && makeReservation(channelId, targetNode, identityNode, identitySecurity)) {

                    Iterator<OutgoingBatch> i = batches.iterator();
                    while (i.hasNext()) {
                        OutgoingBatch batch = i.next();
                        if (OutgoingBatch.Status.inProgress(batch.getStatus())) {
                            OutgoingBatch.Status updatedStatus = updateBatchStatus(batch, targetNode, identityNode, identitySecurity);
                            if (updatedStatus == OutgoingBatch.Status.OK) {
                                i.remove();
                            }
                        }
                    }

                    for (OutgoingBatch batch : batches) {
                        // TODO used to refresh batch if x seconds had passed
                        // since querying. is this necessary?

                        dataExtractorService.extractToStaging(processInfo, targetNode, batch);

                        if (pushWorker == null) {
                            pushWorker = new NodeChannelTransportForPushWorker(channelId, targetNode, identityNode, identitySecurity, status);
                            nodeChannelTransportForPushWorker.execute(pushWorker);
                        }

                        pushWorker.queueUpSend(batch);

                    }
                }
            } catch (Exception ex) {
                error = ex;
                log.error("", ex);
            } finally {
                try {
                    if (pushWorker != null) {
                        pushWorker.queueUpSend(new EOM());
                        pushWorker.waitForComplete();
                    }
                } finally {
                    clusterService.unlockNodeChannel(ClusterConstants.PUSH, nodeChannel.getNodeId(), nodeChannel.getChannelId());
                    processInfo.setStatus(error == null ? ProcessInfo.Status.OK : ProcessInfo.Status.ERROR);
                    pushWorkersWorking.remove(nodeChannel);
                    status.setComplete(true);
                    log.info("Done pushing for {} ", nodeChannel);
                }
            }
        }

    }

    protected boolean makeReservation(String channelId, Node targetNode, Node identityNode, NodeSecurity identitySecurity) {
        ITransportManager transportManager = engine.getTransportManager();
        try {
            transportManager.makeReservationTransport("push", channelId, targetNode, identityNode, identitySecurity.getNodePassword(),
                    parameterService.getRegistrationUrl());
            return true;
        } catch (ServiceUnavailableException ex) {
            log.info("Unable to push to {} on the {} channel.  The service is currently unavailable.", targetNode.getNodeId(), channelId);
            return false;
        } catch (ConnectionRejectedException ex) {
            log.info("Unable to push to {} on the {} channel.  The service must be busy.", targetNode.getNodeId(), channelId);
            return false;
        } catch (ChannelDisabledException ex) {
            log.info("Unable to push to {} on the {} channel.  The channel is disabled at the target.", targetNode.getNodeId(), channelId);
            return false;
        }
    }

    protected Status updateBatchStatus(OutgoingBatch batch, Node targetNode, Node identityNode, NodeSecurity identitySecurity) {
        OutgoingBatch.Status returnStatus = batch.getStatus();
        ITransportManager transportManager = engine.getTransportManager();
        IAcknowledgeService acknowledgeService = engine.getAcknowledgeService();
        IIncomingTransport transport = null;
        try {
            transport = transportManager.getAckStatusTransport(batch, targetNode, identityNode, identitySecurity.getNodePassword(),
                    parameterService.getRegistrationUrl());
            BufferedReader reader = transport.openReader();
            String line = null;
            do {
                line = reader.readLine();
                if (line != null) {
                    log.info("Updating batch status: {}", line);
                    List<BatchAck> batchAcks = transportManager.readAcknowledgement(line, "");
                    for (BatchAck batchInfo : batchAcks) {
                        if (batchInfo.getBatchId() == batch.getBatchId()) {
                            acknowledgeService.ack(batchInfo);
                            returnStatus = batchInfo.getStatus();
                        }
                    }
                }
            } while (line != null);
        } catch (FileNotFoundException ex) {
            log.info("Failed to read batch status for {}.  It is probably because the server is not online yet", batch.getNodeBatchId());
        } catch (Exception ex) {
            log.warn(String.format("Failed to read the batch status for %s", batch.getNodeBatchId()), ex);
        } finally {
            transport.close();
        }
        return returnStatus;
    }

    class NodeChannelTransportForPushWorker implements Runnable {

        CountDownLatch latch = new CountDownLatch(1);

        LinkedBlockingQueue<OutgoingBatch> sendQueue = new LinkedBlockingQueue<OutgoingBatch>();

        Node targetNode;

        Node identityNode;

        NodeSecurity identitySecurity;

        RemoteNodeStatus status;
        
        String channelId;
        
        public NodeChannelTransportForPushWorker(String channelId, Node remoteNode, Node identityNode, NodeSecurity identitySecurity, RemoteNodeStatus status) {
            this.targetNode = remoteNode;
            this.identityNode = identityNode;
            this.identitySecurity = identitySecurity;
            this.status = status;
            this.channelId = channelId;
        }

        public void queueUpSend(OutgoingBatch batch) {
            try {
                sendQueue.put(batch);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void run() {
            IDataExtractorService dataExtractorService = engine.getDataExtractorService();
            ITransportManager transportManager = engine.getTransportManager();
            IAcknowledgeService acknowledgeService = engine.getAcknowledgeService();
            IStatisticManager statisticManager = engine.getStatisticManager();
            
            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(identitySecurity.getNodeId(), targetNode.getNodeId(), 
                    ProcessType.TRANSFER_TO, channelId));

            IOutgoingWithResponseTransport transport = null;
            OutputStream os = null;
            List<OutgoingBatch> batchesSent = new ArrayList<OutgoingBatch>();
            try {
                OutgoingBatch batch = sendQueue.take();
                transport = transportManager.getPushTransport(targetNode, identityNode, identitySecurity.getNodePassword(),
                        batch.getChannelId(), parameterService.getRegistrationUrl());
                while (!(batch instanceof EOM)) {
                    log.info("sending batch {}", batch);
                    processInfo.setCurrentBatchId(batch.getBatchId());
                    processInfo.setCurrentBatchStartTime(new Date());
                    processInfo.setStatus(ProcessInfo.Status.TRANSFERRING);
                    batchesSent.add(batch);
                    IStagedResource resource = dataExtractorService.getStagedResource(batch);
                    InputStream is = resource.getInputStream();
                    if (os == null) {
                        os = transport.openStream();
                    }
                    try {
                        IOUtils.copy(is, os);
                    } finally {
                        resource.close();
                    }
                    batch = sendQueue.take();
                }
                
                processInfo.setStatus(ProcessInfo.Status.OK);

                BufferedReader reader = transport.readResponse();
                
                String line = null;
                do {
                    line = reader.readLine();
                    if (isNotBlank(line)) {
                        log.info("Received ack info: {}", line);
                        List<BatchAck> batchAcks = transportManager.readAcknowledgement(line, "");
                        for (BatchAck batchInfo : batchAcks) {
                            log.info("Saving ack: {}, {}", batchInfo.getBatchId(), batchInfo.getStatus());
                            acknowledgeService.ack(batchInfo);
                        }
                        status.updateOutgoingStatus(batchesSent, batchAcks);
                    }
                } while (line != null);                

            } catch (Exception ex) {                
                processInfo.setStatus(ProcessInfo.Status.ERROR);
                fireOffline(ex, targetNode, status);
                log.error("", ex);
            } finally {
                close(transport);
                latch.countDown();
            }
        }

        public void waitForComplete() {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        private void close(IOutgoingWithResponseTransport transport) {
            try {
                if (transport != null) {
                    transport.close();
                }
            } catch (Exception e) {

            }
        }
    }

    class EOM extends OutgoingBatch {
        private static final long serialVersionUID = 1L;
    }

}
