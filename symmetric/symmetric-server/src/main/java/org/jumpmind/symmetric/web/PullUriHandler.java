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
package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.log.Log;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

/**
 * Handles data pulls from other nodes.
 */
public class PullUriHandler extends AbstractCompressionUriHandler {

    private INodeService nodeService;

    private IConfigurationService configurationService;

    private IDataExtractorService dataExtractorService;

    private IRegistrationService registrationService;
    
    private IStatisticManager statisticManager;
    
    public PullUriHandler(Log log, IParameterService parameterService,
            INodeService nodeService,
            IConfigurationService configurationService, IDataExtractorService dataExtractorService,
            IRegistrationService registrationService, IStatisticManager statisticManager,  IInterceptor... interceptors) {
        super(log, "/pull/*", parameterService, interceptors);
        this.nodeService = nodeService;
        this.configurationService = configurationService;
        this.dataExtractorService = dataExtractorService;
        this.registrationService = registrationService;
        this.statisticManager = statisticManager;
    }


    public void handleWithCompression(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        // request has the "other" nodes info
        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);
        
        log.debug("ServletPulling", nodeId);

        if (StringUtils.isBlank(nodeId)) {
            ServletUtils.sendError(res, HttpServletResponse.SC_BAD_REQUEST, "Node must be specified");
            return;
        }

        ChannelMap map = new ChannelMap();
        map.addSuspendChannels(req.getHeader(WebConstants.SUSPENDED_CHANNELS));
        map.addIgnoreChannels(req.getHeader(WebConstants.IGNORED_CHANNELS));

        // pull out headers and pass to pull() method

        pull(nodeId, req.getRemoteHost(), req.getRemoteAddr(), res.getOutputStream(), map);

        log.debug("ServletPulled", nodeId);

    }
    
    
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