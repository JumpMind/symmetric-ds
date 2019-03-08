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
package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;

/**
 * Handles data pushes from nodes.
 */
public class PushUriHandler extends AbstractUriHandler {

    private IDataLoaderService dataLoaderService;

    private IStatisticManager statisticManager;
    
    private INodeService nodeService;
    
    public PushUriHandler(IParameterService parameterService, IDataLoaderService dataLoaderService,
            IStatisticManager statisticManager, INodeService nodeService,
            IInterceptor... interceptors) {
        super("/push/*", parameterService, interceptors);
        this.dataLoaderService = dataLoaderService;
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {

        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);
        log.debug("Push requested for {}", nodeId);
        InputStream inputStream = createInputStream(req);
        OutputStream outputStream = res.getOutputStream();

        String threadChannel = req.getHeader(WebConstants.CHANNEL_QUEUE);
        
        int rc = push(nodeId, threadChannel, inputStream, outputStream);
        
        if (rc != WebConstants.SC_OK) {
            res.sendError(rc);
        }

        res.flushBuffer();
        log.debug("Push completed for {}", nodeId);
    }
    
    protected int push(String sourceNodeId, String channelId, InputStream inputStream, OutputStream outputStream) throws IOException {
        long ts = System.currentTimeMillis();
        try {
            Node sourceNode = nodeService.findNode(sourceNodeId, true);
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(sourceNodeId, true);
            
            if (nodeSecurity != null) {
                String createdAtNodeId = nodeSecurity.getCreatedAtNodeId();
                if (nodeSecurity.isRegistrationEnabled() && (createdAtNodeId == null || createdAtNodeId.equals(nodeService.findIdentityNodeId()))) {
                    return WebConstants.REGISTRATION_REQUIRED;
                }
            }
            dataLoaderService.loadDataFromPush(sourceNode, channelId, inputStream, outputStream);
        } finally {
            statisticManager.incrementNodesPushed(1);
            statisticManager.incrementTotalNodesPushedTime(System.currentTimeMillis() - ts);
        }
        return WebConstants.SC_OK;
    }

    protected InputStream createInputStream(HttpServletRequest req) throws IOException {
        InputStream is = null;
        String contentType = req.getHeader("Content-Type");
        boolean useCompression = contentType != null && contentType.equalsIgnoreCase("gzip");
        is = req.getInputStream();
        if (useCompression) {
            is = new GZIPInputStream(is);
        }
        return is;
    }
    
}
