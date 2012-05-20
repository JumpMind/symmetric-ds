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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataLoaderService;
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
public class PullService extends AbstractOfflineDetectorService implements IPullService {

    private INodeService nodeService;

    private IDataLoaderService dataLoaderService;

    private IRegistrationService registrationService;

    private IClusterService clusterService;

    private Map<String, Date> startTimesOfNodesBeingPulled = new HashMap<String, Date>();

    public PullService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            INodeService nodeService, IDataLoaderService dataLoaderService,
            IRegistrationService registrationService, IClusterService clusterService) {
        super(parameterService, symmetricDialect);
        this.nodeService = nodeService;
        this.dataLoaderService = dataLoaderService;
        this.registrationService = registrationService;
        this.clusterService = clusterService;
    }

    public Map<String, Date> getStartTimesOfNodesBeingPulled() {
        return new HashMap<String, Date>(startTimesOfNodesBeingPulled);
    }

    synchronized public RemoteNodeStatuses pullData() {
        RemoteNodeStatuses statuses = new RemoteNodeStatuses();
        Node identity = nodeService.findIdentity(false);
        if (identity == null || identity.isSyncEnabled()) {
            if (clusterService.lock(ClusterConstants.PULL)) {
                try {
                    // register if we haven't already been registered
                    registrationService.registerWithServer();

                    identity = nodeService.findIdentity(false);

                    if (identity != null) {

                        List<Node> nodes = nodeService.findNodesToPull();
                        if (nodes != null && nodes.size() > 0) {
                            for (Node node : nodes) {
                                if (StringUtils.isNotBlank(node.getNodeId())
                                        && !node.getNodeId().equals(identity.getNodeId())) {
                                    if (StringUtils.isNotBlank(node.getSyncUrl())) {
                                        RemoteNodeStatus status = statuses.add(node);
                                        try {
                                            startTimesOfNodesBeingPulled.put(node.getNodeId(),
                                                    new Date());
                                            log.debug("Pull requested for {}", node.toString());
                                            dataLoaderService.loadDataFromPull(node, status);
                                            if (status.getDataProcessed() > 0
                                                    || status.getBatchesProcessed() > 0) {
                                                log.info(
                                                        "Pull data received from {}.  {} rows and {} batches were processed",
                                                        new Object[] { node.toString(),
                                                                status.getDataProcessed(),
                                                                status.getBatchesProcessed() });
                                            } else {
                                                log.debug(
                                                        "Pull data received from {}.  {} rows and {} batches were processed",
                                                        new Object[] { node.toString(),
                                                                status.getDataProcessed(),
                                                                status.getBatchesProcessed() });
                                            }
                                        } catch (ConnectException ex) {
                                            log.warn(
                                                    "TransportFailedConnectionUnavailable",
                                                    (node.getSyncUrl() == null ? parameterService
                                                            .getRegistrationUrl() : node
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
                                        } finally {
                                            startTimesOfNodesBeingPulled.remove(node.getNodeId());
                                        }
                                    } else {
                                        log.warn(
                                                "Cannot pull node '{}' in the group '{}'.  The sync url is blank",
                                                node.getNodeId(), node.getNodeGroupId());
                                    }
                                }
                            }
                        }
                    }
                } finally {
                    clusterService.unlock(ClusterConstants.PULL);

                }
            } else {
                log.info("Did not run the pull process because the cluster service has it locked");
            }
        }

        return statuses;
    }

}
