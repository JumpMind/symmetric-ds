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

import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager.ReservationType;

/**
 * An interceptor that controls access to this node for pushes and pulls. It is
 * configured within symmetric-web.xml
 */
public class NodeConcurrencyInterceptor implements IInterceptor {

    private IConcurrentConnectionManager concurrentConnectionManager;

    private IConfigurationService configurationService;

    private IStatisticManager statisticManager;
    
    public NodeConcurrencyInterceptor(IConcurrentConnectionManager concurrentConnectionManager,
            IConfigurationService configurationService, IStatisticManager statisticManager) {
        this.concurrentConnectionManager = concurrentConnectionManager;
        this.configurationService = configurationService;
        this.statisticManager = statisticManager;
    }

    public boolean before(HttpServletRequest req, HttpServletResponse resp) throws IOException,
            ServletException {
        String poolId = req.getRequestURI();
        String nodeId = getNodeId(req);
        String method = req.getMethod();

        if (method.equals(WebConstants.METHOD_HEAD) && 
                ServletUtils.normalizeRequestUri(req).contains("push")) {
            // I read here:
            // http://java.sun.com/j2se/1.5.0/docs/guide/net/http-keepalive.html
            // that keepalive likes to have a known content length. I also read
            // that HEAD is better if no content is going to be returned.
            resp.setContentLength(0);
            if (!concurrentConnectionManager
                    .reserveConnection(nodeId, poolId, ReservationType.SOFT)) {
                statisticManager.incrementNodesRejected(1);
                ServletUtils.sendError(resp, WebConstants.SC_SERVICE_UNAVAILABLE);
            } else {
                buildSuspendIgnoreResponseHeaders(nodeId, resp);
            }
            return false;
        } else if (concurrentConnectionManager.reserveConnection(nodeId, poolId,
                ReservationType.HARD)) {
                buildSuspendIgnoreResponseHeaders(nodeId, resp);
                return true;
        } else {
            statisticManager.incrementNodesRejected(1);
            ServletUtils.sendError(resp, WebConstants.SC_SERVICE_UNAVAILABLE);
            return false;
        }
    }
    
    protected String getNodeId(HttpServletRequest req) {
        String nodeId = StringUtils.trimToNull(req.getParameter(WebConstants.NODE_ID));
        if (StringUtils.isBlank(nodeId)) {
        	// if this is a registration request, we won't have a node id to use. 
        	nodeId = StringUtils.trimToNull(req.getParameter(WebConstants.EXTERNAL_ID));
        }
        return nodeId;
    }
    
    public void after(HttpServletRequest req, HttpServletResponse resp) throws IOException,
            ServletException {
        String poolId = req.getRequestURI();
        String nodeId = getNodeId(req);
        concurrentConnectionManager.releaseConnection(nodeId, poolId);
    }

    protected void buildSuspendIgnoreResponseHeaders(final String nodeId, final ServletResponse resp) {
        HttpServletResponse httpResponse = (HttpServletResponse) resp;
        ChannelMap suspendIgnoreChannels = configurationService
                .getSuspendIgnoreChannelLists(nodeId);
        httpResponse.setHeader(WebConstants.SUSPENDED_CHANNELS,
                suspendIgnoreChannels.getSuspendChannelsAsString());
        httpResponse.setHeader(WebConstants.IGNORED_CHANNELS,
                suspendIgnoreChannels.getIgnoreChannelsAsString());
    }

}