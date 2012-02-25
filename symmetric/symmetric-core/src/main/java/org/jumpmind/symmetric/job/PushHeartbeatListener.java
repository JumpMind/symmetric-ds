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
package org.jumpmind.symmetric.job;

import java.util.Set;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;

public class PushHeartbeatListener implements IHeartbeatListener {

    private IDataService dataService;
    private INodeService nodeService;
    private ISymmetricDialect symmetricDialect;
    private IParameterService parameterService;

    public PushHeartbeatListener(IParameterService parameterService, IDataService dataService,
            INodeService nodeService, ISymmetricDialect symmetricDialect) {
        this.parameterService = parameterService;
        this.dataService = dataService;
        this.nodeService = nodeService;
        this.symmetricDialect = symmetricDialect;
    }

    public void heartbeat(Node me, Set<Node> children) {
        if (parameterService.is(ParameterConstants.HEARTBEAT_ENABLED)) {
            // don't send new heart beat events if we haven't sent
            // the last ones ...
            if (!nodeService.isRegistrationServer()) {
                if (!symmetricDialect.getPlatform().getPlatformInfo().isTriggersSupported()) {
                    dataService.insertHeartbeatEvent(me, false);
                    for (Node node : children) {
                        dataService.insertHeartbeatEvent(node, false);
                    }
                }
            }
        }
    }

    public long getTimeBetweenHeartbeatsInSeconds() {
        return parameterService.getLong(ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC, 600);
    }

}