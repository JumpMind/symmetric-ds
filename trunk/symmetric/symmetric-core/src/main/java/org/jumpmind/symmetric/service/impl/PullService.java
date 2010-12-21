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

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.List;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.AuthenticationException;
import org.jumpmind.symmetric.transport.ConnectionRejectedException;
import org.jumpmind.symmetric.transport.SyncDisabledException;
import org.jumpmind.symmetric.transport.TransportException;

/**
 * 
 */
public class PullService extends AbstractOfflineDetectorService implements IPullService {

    private INodeService nodeService;

    private IDataLoaderService dataLoaderService;

    private IRegistrationService registrationService;

    private IClusterService clusterService;

    synchronized public boolean pullData() {
        boolean dataPulled = false;
        if (clusterService.lock(ClusterConstants.PULL)) {
            try {
                // register if we haven't already been registered
                registrationService.registerWithServer();

                List<Node> nodes = nodeService.findNodesToPull();
                if (nodes != null && nodes.size() > 0) {
                    for (Node node : nodes) {
                        String nodeName = " for " + node;
                        try {
                            log.debug("DataPulling", nodeName);
                            if (dataLoaderService.loadDataFromPull(node)) {
                                log.info("DataPulled", nodeName);
                                dataPulled = true;
                            } else {
                                log.debug("DataPullingFailed", nodeName);

                            }
                        } catch (ConnectException ex) {
                            log.warn("TransportFailedConnectionUnavailable",
                                    (node.getSyncUrl() == null ? parameterService.getRegistrationUrl() : node
                                            .getSyncUrl()));
                            fireOffline(ex, node);
                        } catch (ConnectionRejectedException ex) {
                            log.warn("TransportFailedConnectionBusy");
                            fireOffline(ex, node);
                        } catch (AuthenticationException ex) {
                            log.warn("AuthenticationFailed");
                            fireOffline(ex, node);
                        } catch (SyncDisabledException ex) {
                            log.warn("SyncDisabled");
                            fireOffline(ex, node);
                        } catch (SocketException ex) {
                            log.warn("Message", ex.getMessage());
                            fireOffline(ex, node);
                        } catch (TransportException ex) {
                            log.warn("Message", ex.getMessage());
                            fireOffline(ex, node);
                        } catch (IOException ex) {
                            log.error(ex);
                            fireOffline(ex, node);
                        }
                    }
                }
            } finally {
                clusterService.unlock(ClusterConstants.PULL);

            }

        } else {
            log.info("DataPullingFailedLock");
        }

        return dataPulled;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setDataLoaderService(IDataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

}