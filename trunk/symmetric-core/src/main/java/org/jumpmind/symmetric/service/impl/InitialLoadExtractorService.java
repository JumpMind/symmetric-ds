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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.model.NodeCommunication.CommunicationType;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IInitialLoadExtractorService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;

public class InitialLoadExtractorService extends AbstractService implements
        IInitialLoadExtractorService, INodeCommunicationExecutor {

    private IDataService dataService;

    private IDataExtractorService dataExtractorService;

    private INodeCommunicationService nodeCommunicationService;

    private IClusterService clusterService;

    private INodeService nodeService;

    private ITriggerRouterService triggerRouterService;

    private IConfigurationService configurationService;

    private boolean syncTriggersBeforeInitialLoadAttempted = false;

    public InitialLoadExtractorService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IDataExtractorService dataExtractorService,
            IDataService dataService, INodeCommunicationService nodeCommunicationService,
            IClusterService clusterService, INodeService nodeService,
            ITriggerRouterService triggerRouterService, IConfigurationService configurationService) {
        super(parameterService, symmetricDialect);
        this.dataService = dataService;
        this.dataExtractorService = dataExtractorService;
        this.nodeCommunicationService = nodeCommunicationService;
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        this.triggerRouterService = triggerRouterService;
        this.configurationService = configurationService;
    }

    public void queueWork(boolean force) {
        Node identity = nodeService.findIdentity();
        if (identity != null) {
            if (force || !clusterService.isInfiniteLocked(ClusterConstants.INITIAL_LOAD_EXTRACT)) {
                NodeSecurity identitySecurity = nodeService.findNodeSecurity(identity.getNodeId());
                if (parameterService.isRegistrationServer()
                        || (identitySecurity != null && !identitySecurity.isRegistrationEnabled() && identitySecurity
                                .getRegistrationTime() != null)) {
                    List<NodeSecurity> nodeSecurities = nodeService
                            .findNodeSecurityWithLoadEnabled();
                    if (nodeSecurities != null) {
                        boolean reverseLoadFirst = parameterService
                                .is(ParameterConstants.INTITAL_LOAD_REVERSE_FIRST);
                        for (NodeSecurity security : nodeSecurities) {
                            if (triggerRouterService.getActiveTriggerHistories().size() > 0) {
                                boolean reverseLoadQueued = security.isRevInitialLoadEnabled();
                                boolean initialLoadQueued = security.isInitialLoadEnabled();
                                boolean thisMySecurityRecord = security.getNodeId().equals(
                                        identity.getNodeId());
                                boolean registered = security.getRegistrationTime() != null;
                                boolean parent = identity.getNodeId().equals(
                                        security.getCreatedAtNodeId());
                                if (thisMySecurityRecord && reverseLoadQueued
                                        && (reverseLoadFirst || !initialLoadQueued)) {
                                    // queue up reverse initial load
                                } else if (!thisMySecurityRecord && registered && parent
                                        && initialLoadQueued
                                        && (!reverseLoadFirst || !reverseLoadQueued)) {
                                    // queue up initial load
                                }
                            } else {
                                List<NodeGroupLink> links = configurationService
                                        .getNodeGroupLinksFor(parameterService.getNodeGroupId());
                                if (links == null || links.size() == 0) {
                                    log.warn(
                                            "Could not queue up a load for {} because a node group link is NOT configured over which a load could be delivered",
                                            security.getNodeId());
                                } else {
                                    log.warn(
                                            "Could not queue up a load for {} because sync triggers has not yet run",
                                            security.getNodeId());
                                    if (!syncTriggersBeforeInitialLoadAttempted) {
                                        syncTriggersBeforeInitialLoadAttempted = true;
                                        triggerRouterService.syncTriggers();
                                    }
                                }
                            }
                        }
                    } else {

                    }
                } else {
                    log.debug("Not running intiial load extract service because the node is not registered");
                }
            } else {
                log.debug("Not running intiial load extract service because the job has been disabled");
            }
        } else {
            log.debug("Not running initial load extract service because this node does not have an identity");
        }

        /*
         * Called by the load extract job to queue up load requests. The logic
         * should be similar to RouterService.insertInitialLoadEvents to figure
         * out which nodes need a load extracted.
         * 
         * Look at FileSyncService.queueJob to see how to queue up work using
         * the NodeCommunicationService (there is probably some duplicate code
         * across PullService, PushService and FileSyncService)
         */
    }

    protected RemoteNodeStatuses queueJob(long minimumPeriodMs, CommunicationType type) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses();
        List<NodeCommunication> nodes = nodeCommunicationService.list(type);
        int availableThreads = nodeCommunicationService.getAvailableThreads(type);
        for (NodeCommunication nodeCommunication : nodes) {
            if (StringUtils.isNotBlank(nodeCommunication.getNode().getSyncUrl())
                    || !parameterService.isRegistrationServer()) {
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
            } else {
                log.warn(
                        "File sync cannot communicate with node '{}' in the group '{}'.  The sync url is blank",
                        nodeCommunication.getNode().getNodeId(), nodeCommunication.getNode()
                                .getNodeGroupId());
            }
        }

        return statuses;
    }

    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {
        /*
         * This is where the initial load events are inserted into sym_data.
         * 
         * DataService.insertReloadEvents currently inserts reload events. We
         * would probably deprecate the method and move the logic here.
         * 
         * As we iterate over the list of TriggerRouters that need to be
         * extracted we should check to see if an extract has already occurred
         * (in the case of a restart midway through an initial load extraction).
         * Each table that was extracted should have a done file.
         * 
         * Loop:
         * 
         * Use ILoadExtract to extract files. This should be configured in
         * sym_trigger. We can add a column names load_extract_type
         * 
         * --- The default implementation will create a DataProcessor that reads
         * its data using ExtractDataReader and SelectFromTableSource and writes
         * its data using a new writer named MultipleFileExtractWriter.
         * MultipleFileExtractWriter will use IStagingManager to get
         * IStagedResources (see FileSyncZipDataWriter for an example of using a
         * IStagedResource that always writes to a file). After the
         * DataProcessor has run the MultipleFileExtractWriter will have a
         * getFiles() method to get a handle to the files that were written. ---
         * 
         * Insert each set of sym_data events after the table is extracted and
         * write the done file.
         * 
         * End Loop
         * 
         * Set sym_node_security.initial_load_enabled=0,
         * initial_load_time=current_timestamp
         */
    }

    interface ILoadExtract extends IExtensionPoint {

        public List<File> extract(Node targetNode, TriggerRouter triggerRouter,
                LoadExtractFileHandleFactory fileHandleFactory);

    }

    class LoadExtractFileHandleFactory {
        public File getFileName(int loadId, Node targetNode, TriggerRouter triggerRouter,
                int fileNumber) {
            return null;
        }
    }

}
