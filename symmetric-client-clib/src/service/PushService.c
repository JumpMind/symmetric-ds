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
#include "service/PushService.h"

SymRemoteNodeStatus * SymPushService_pushData(SymPushService *this) {
    return NULL;
}

SymRemoteNodeStatus * pushData(SymPushService *this, unsigned int force) {
/*
    RemoteNodeStatuses statuses = new RemoteNodeStatuses(configurationService.getChannels(false));

    Node identity = nodeService.findIdentity(false);
    if (identity != null && identity.isSyncEnabled()) {
        long minimumPeriodMs = parameterService.getLong(ParameterConstants.PUSH_MINIMUM_PERIOD_MS, -1);
        if (force || !clusterService.isInfiniteLocked(ClusterConstants.PUSH)) {
                List<NodeCommunication> nodes = nodeCommunicationService
                        .list(CommunicationType.PUSH);
                if (nodes.size() > 0) {
                    NodeSecurity identitySecurity = nodeService.findNodeSecurity(identity
                            .getNodeId());
                    if (identitySecurity != null) {
                        int availableThreads = nodeCommunicationService
                                .getAvailableThreads(CommunicationType.PUSH);
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
                    } else {
                        log.error(
                                "Could not find a node security row for '{}'.  A node needs a matching security row in both the local and remote nodes if it is going to authenticate to push data",
                                identity.getNodeId());
                    }
                }
        } else {
            log.debug("Did not run the push process because it has been stopped");
        }
    }
    return statuses;
*/
    return NULL;
}

void SymPushService_destroy(SymPushService *this) {
    free(this);
}

SymPushService * SymPushService_new(SymPushService *this) {
    if (this == NULL) {
        this = (SymPushService *) calloc(1, sizeof(SymPushService));
    }
    this->pushData = (void *) &SymPushService_pushData;
    this->destroy = (void *) &SymPushService_destroy;
    return this;
}
