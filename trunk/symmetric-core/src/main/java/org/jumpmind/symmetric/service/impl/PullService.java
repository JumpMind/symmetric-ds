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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.transport.TransportException;

/**
 * @see IPullService
 */
public class PullService extends AbstractOfflineDetectorService implements IPullService, INodeCommunicationExecutor {

    private INodeService nodeService;

    private IRegistrationService registrationService;

    private IClusterService clusterService;

    private INodeCommunicationService nodeCommunicationService;
    
    private IDataLoaderService dataLoaderService;

    public PullService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            INodeService nodeService, IDataLoaderService dataLoaderService,
            IRegistrationService registrationService, IClusterService clusterService,
            INodeCommunicationService nodeCommunicationService) {
        super(parameterService, symmetricDialect);
        this.nodeService = nodeService;
        this.registrationService = registrationService;
        this.clusterService = clusterService;
        this.nodeCommunicationService = nodeCommunicationService;
        this.dataLoaderService = dataLoaderService;
    }

    synchronized public RemoteNodeStatuses pullData(boolean force) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses();
        Node identity = nodeService.findIdentity(false);
        if (identity == null || identity.isSyncEnabled()) {
            if (force || clusterService.lock(ClusterConstants.PULL)) {
                try {
                    // register if we haven't already been registered
                    registrationService.registerWithServer();
                    identity = nodeService.findIdentity(false);
                    if (identity != null) {
                        List<NodeCommunication> nodes = nodeCommunicationService
                                .list(CommunicationType.PULL);
                        int availableThreads = nodeCommunicationService
                                .getAvailableThreads(CommunicationType.PULL);
                        for (NodeCommunication nodeCommunication : nodes) {
                            if (availableThreads > 0 && !nodeCommunication.isLocked()) {                                
                                nodeCommunicationService.execute(nodeCommunication, statuses,
                                        this);
                                availableThreads--;
                            }
                        }
                    }
                } finally {
                    if (!force) {
                        clusterService.unlock(ClusterConstants.PULL);
                    }
                }
            } else {
                log.info("Did not run the pull process because the cluster service has it locked");
            }
        }

        return statuses;
    }
    
    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        Node node = nodeCommunication.getNode();
        if (StringUtils.isNotBlank(node.getSyncUrl())) {
            try {
                int pullCount = 0;
                long batchesProcessedCount = 0;
                do {
                    batchesProcessedCount = status.getBatchesProcessed();
                    pullCount++;
                    log.debug("Pull requested for {}", node.toString());
                    if (pullCount > 1) {
                        log.info("Immediate pull requested while in reload mode");
                    }
                    
                    dataLoaderService.loadDataFromPull(node, status);
                    
                    if (status.getDataProcessed() > 0 || status.getBatchesProcessed() > 0) {
                        log.info(
                                "Pull data received from {}.  {} rows and {} batches were processed",
                                new Object[] { node.toString(), status.getDataProcessed(),
                                        status.getBatchesProcessed() });

                    } else {
                        log.debug(
                                "Pull data received from {}.  {} rows and {} batches were processed",
                                new Object[] { node.toString(), status.getDataProcessed(),
                                        status.getBatchesProcessed() });
                    }
                    /*
                     * Re-pull immediately if we are in the middle of an initial
                     * load so that the initial load completes as quickly as
                     * possible.
                     */
                } while (nodeService.isDataLoadStarted() && !status.failed()
                        && status.getBatchesProcessed() > batchesProcessedCount);
            } catch (ConnectException ex) {
                log.warn(
                        "Failed to connect to the transport: ",
                        (node.getSyncUrl() == null ? parameterService.getRegistrationUrl() : node
                                .getSyncUrl()));
                fireOffline(ex, node, status);
            } catch (ConnectionRejectedException ex) {
                log.warn("The server was too busy to accept the connection");
                fireOffline(ex, node, status);
            } catch (AuthenticationException ex) {
                log.warn("Could not authenticate with node");
                fireOffline(ex, node, status);
            } catch (SyncDisabledException ex) {
                log.warn("Synchronization is disabled on the server node");
                fireOffline(ex, node, status);
            } catch (SocketException ex) {
                log.warn("{}", ex.getMessage());
                fireOffline(ex, node, status);
            } catch (TransportException ex) {
                log.warn("{}", ex.getMessage());
                fireOffline(ex, node, status);
            } catch (IOException ex) {
                log.error(ex.getMessage(), ex);
                fireOffline(ex, node, status);
            }
        } else {
            log.warn("Cannot pull node '{}' in the group '{}'.  The sync url is blank",
                    node.getNodeId(), node.getNodeGroupId());
        }
    }


}
