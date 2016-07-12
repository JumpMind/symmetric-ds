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
package org.jumpmind.symmetric.job;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.transport.IOutgoingWithResponseTransport;
import org.jumpmind.symmetric.transport.http.HttpOutgoingTransport;
import org.jumpmind.symmetric.web.WebConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class ReportStatusJob extends AbstractJob {

    protected ReportStatusJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
            super("job.report.status", true, engine.getParameterService().is(ParameterConstants.HYBRID_PUSH_PULL_ENABLED),
                engine, taskScheduler);
    }

    @Override
    void doJob(boolean force) throws Exception {
        Node identity = engine.getNodeService().findIdentity();
        
        int batchesToSend = engine.getOutgoingBatchService().countOutgoingBatchesPending(identity.getNodeId());
        
        if (batchesToSend > 0) {            
            NodeSecurity identitySecurity = engine.getNodeService().findNodeSecurity(identity.getNodeId(), true);
            
            NetworkedNode remote = engine.getNodeService().getRootNetworkedNode();
            
            Map<String, String> requestParams = new HashMap<String, String>();
            requestParams.put(WebConstants.BATCH_TO_SEND_COUNT, String.valueOf(batchesToSend));
            
            IOutgoingWithResponseTransport transport = engine.getTransportManager().getPushTransport(remote.getNode(), 
                    identity,
                    identitySecurity.getNodePassword(), requestParams, engine.getParameterService().getRegistrationUrl());
            
            if (transport instanceof HttpOutgoingTransport) {
                HttpOutgoingTransport httpTransport = (HttpOutgoingTransport) transport;
                httpTransport.openStream().close(); // Effectively just sending over a header.
            }
        }
        
        
    }
    
    @Override
    public String getClusterLockName() {
        return ClusterConstants.REPORT_STATUS;
    }

}
