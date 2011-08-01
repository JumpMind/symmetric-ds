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


package org.jumpmind.symmetric.transport.handler;

import java.io.IOException;
import java.io.OutputStream;

import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.web.PullServlet;

/**
 * Handles data pulls from other nodes.
 * 
 * @see PullServlet
 */
public class PullResourceHandler extends AbstractTransportResourceHandler {

    private INodeService nodeService;

    private IConfigurationService configurationService;

    private IDataExtractorService dataExtractorService;

    private IRegistrationService registrationService;
    
    private IStatisticManager statisticManager;

    public void pull(String nodeId, String remoteHost, String remoteAddress,
            OutputStream outputStream, ChannelMap map) throws IOException {
        INodeService nodeService = getNodeService();
        NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId);
        long ts = System.currentTimeMillis();
        try {
            ChannelMap remoteSuspendIgnoreChannelsList = configurationService
                    .getSuspendIgnoreChannelLists(nodeId);
            map.addSuspendChannels(remoteSuspendIgnoreChannelsList.getSuspendChannels());
            map.addIgnoreChannels(remoteSuspendIgnoreChannelsList.getIgnoreChannels());

            if (nodeSecurity != null) {
                if (nodeSecurity.isRegistrationEnabled()) {
                    registrationService.registerNode(nodeService.findNode(nodeId), remoteHost,
                            remoteAddress, outputStream, false);
                } else {
                    IOutgoingTransport outgoingTransport = createOutgoingTransport(outputStream,
                            map);
                    dataExtractorService.extract(nodeService.findNode(nodeId), outgoingTransport);
                    outgoingTransport.close();
                }
            } else {
                log.warn("NodeMissing", nodeId);
            }
        } finally {
            statisticManager.incrementNodesPulled(1);
            statisticManager.incrementTotalNodesPulledTime(System.currentTimeMillis() - ts);
        }
    }

    private INodeService getNodeService() {
        return nodeService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public IConfigurationService getConfigurationService() {
        return configurationService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }
}