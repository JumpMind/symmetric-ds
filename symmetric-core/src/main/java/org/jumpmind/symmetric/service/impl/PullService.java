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

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IRegistrationService;

/**
 * @see IPullService
 */
public class PullService extends AbstractOfflineDetectorService implements IPullService, INodeCommunicationExecutor {

    private INodeService nodeService;

    private IRegistrationService registrationService;

    private IClusterService clusterService;

    private INodeCommunicationService nodeCommunicationService;
    
    private IDataLoaderService dataLoaderService;
    
    private IConfigurationService configurationService;

    public PullService(IParameterService parameterService, ISymmetricDialect symmetricDialect,
            INodeService nodeService, IDataLoaderService dataLoaderService,
            IRegistrationService registrationService, IClusterService clusterService,
            INodeCommunicationService nodeCommunicationService, IConfigurationService configurationService,
            IExtensionService extensionService) {
        super(parameterService, symmetricDialect, extensionService);
        this.nodeService = nodeService;
        this.registrationService = registrationService;
        this.clusterService = clusterService;
        this.nodeCommunicationService = nodeCommunicationService;
        this.dataLoaderService = dataLoaderService;
        this.configurationService = configurationService;
    }

    synchronized public RemoteNodeStatuses pullData(boolean force) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses(configurationService.getChannels(false));
        Node identity = nodeService.findIdentity();
        if (identity == null || identity.isSyncEnabled()) {
            long minimumPeriodMs = parameterService.getLong(ParameterConstants.PULL_MINIMUM_PERIOD_MS, -1);
            if (force || !clusterService.isInfiniteLocked(ClusterConstants.PULL)) {
                    // register if we haven't already been registered
                    registrationService.registerWithServer();
                    identity = nodeService.findIdentity();
                    if (identity != null) {
                        List<NodeCommunication> nodes = nodeCommunicationService
                                .list(CommunicationType.PULL);
                        int availableThreads = nodeCommunicationService
                                .getAvailableThreads(CommunicationType.PULL);
                        for (NodeCommunication nodeCommunication : nodes) {
                            boolean meetsMinimumTime = true;
                            if (minimumPeriodMs > 0 && nodeCommunication.getLastLockTime() != null &&
                               (System.currentTimeMillis() - nodeCommunication.getLastLockTime().getTime()) < minimumPeriodMs) {
                               meetsMinimumTime = false; 
                            }
                            if (availableThreads > 0 && meetsMinimumTime) {
                                if (nodeCommunicationService.execute(nodeCommunication, statuses,
                                        this)) {
                                    availableThreads--;
                                }
                            }
                        }
                    }
            } else {
                log.debug("Did not run the pull process because it has been stopped");
            }
        }

        return statuses;
    }
    
    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        Node node = nodeCommunication.getNode();
        boolean immediatePullIfDataFound = parameterService.is(ParameterConstants.PULL_IMMEDIATE_IF_DATA_FOUND, false);
        
        if (StringUtils.isNotBlank(node.getSyncUrl()) || !parameterService.isRegistrationServer()) {
            do {
                log.debug("Pull requested for {}", node.toString());
                if (status.getBatchesProcessed() > 0) {
                	if (nodeService.isDataLoadStarted()) {
                		log.info("Immediate pull requested while in reload mode");
                	} else {
                        log.debug("Immediate pull requested while data found");
                    }
                	status.resetCounts();
                }

                try {
                    dataLoaderService.loadDataFromPull(node, status);
                    fireOnline(node, status);
                } catch (Exception ex) {
                    fireOffline(ex, node, status);
                }

                if (!status.failed() && (status.getDataProcessed() > 0 || status.getBatchesProcessed() > 0)) {
                    log.info(
                            "Pull data received from {} {}.  {} rows and {} batches were processed",
                            new Object[] { node.toString(), "on channel thread " + nodeCommunication.getQueue(),
                                    status.getDataProcessed(), status.getBatchesProcessed() });

                } else if (status.failed()) {
                    log.debug(
                            "There was a failure while pulling data from {} {}.  {} rows and {} batches were processed",
                            new Object[] { node.toString(), "on channel thread " + nodeCommunication.getQueue(),
                                    status.getDataProcessed(), status.getBatchesProcessed() });
                }
                /*
                 * Re-pull immediately if we are in the middle of an initial
                 * load so that the initial load completes as quickly as
                 * possible.
                 */
            } while ((immediatePullIfDataFound || nodeService.isDataLoadStarted()) && !status.failed()
                    && status.getBatchesProcessed() > 0);           
        } else {
            log.warn("Cannot pull node '{}' in the group '{}'.  The sync url is blank",
                    node.getNodeId(), node.getNodeGroupId());
        }
    }

    public RemoteNodeStatus pullConfigData(boolean force) {
        Node local = nodeService.findIdentity();
        RemoteNodeStatus status = null;

        if (!parameterService.isRegistrationServer() && local != null && (force || !Version.version().equals(local.getConfigVersion()))) {
            Node remote = new Node();
            remote.setSyncUrl(parameterService.getRegistrationUrl());    
            status = new RemoteNodeStatus(remote.getNodeId(), Constants.CHANNEL_CONFIG, configurationService.getChannels(false));

            try {
                dataLoaderService.loadDataFromConfig(remote, status, force);
            } catch (Exception e) {
                fireOffline(e, remote, status);
            }
        }
        return status;
    }

}
