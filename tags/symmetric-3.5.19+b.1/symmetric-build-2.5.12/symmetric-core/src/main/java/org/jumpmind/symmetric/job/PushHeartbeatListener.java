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
package org.jumpmind.symmetric.job;

import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;

public class PushHeartbeatListener implements IHeartbeatListener, IBuiltInExtensionPoint {

    private boolean enabled;
    private IDataService dataService;
    private INodeService nodeService;
    private IOutgoingBatchService outgoingBatchService;
    private long timeBetweenHeartbeats;
    private IDbDialect dbDialect;

    public void heartbeat(Node me, Set<Node> children) {
        if (enabled) {
            // don't send new heart beat events if we haven't sent
            // the last ones ...
            if (!nodeService.isRegistrationServer() && !isUnsentDataPresentOnConfigChannel()) {
                if (!dbDialect.getPlatform().getPlatformInfo().isTriggersSupported()) {
                    dataService.insertHeartbeatEvent(me, false);
                    for (Node node : children) {
                        dataService.insertHeartbeatEvent(node, false);
                    }
                }
            }
        }
    }
    
    protected boolean isUnsentDataPresentOnConfigChannel() {
        boolean isUnsentDataOnChannel = false;
        List<Node> nodes = nodeService.findNodesToPushTo();
        if (nodes != null) {
            for (Node node : nodes) {
                isUnsentDataOnChannel |= outgoingBatchService.isUnsentDataOnChannelForNode(
                        Constants.CHANNEL_CONFIG, node.getNodeId());
            }
        }
        return isUnsentDataOnChannel;
    }
    
    public long getTimeBetweenHeartbeatsInSeconds() {
        return this.timeBetweenHeartbeats;
    }
    
    public void setTimeBetweenHeartbeats(long timeBetweenHeartbeats) {
        this.timeBetweenHeartbeats = timeBetweenHeartbeats;
    }

    public boolean isAutoRegister() {
        return enabled;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setOutgoingBatchService(IOutgoingBatchService outgoingBatchService) {
        this.outgoingBatchService = outgoingBatchService;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
    
    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }
}