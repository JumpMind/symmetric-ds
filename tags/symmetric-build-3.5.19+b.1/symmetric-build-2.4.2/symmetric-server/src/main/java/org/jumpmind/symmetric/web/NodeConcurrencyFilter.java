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

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager.ReservationType;

/**
 * A Servlet filter that controls access to this node for pushes and pulls.  It is 
 * configured within symmetric-web.xml
 */
public class NodeConcurrencyFilter extends AbstractFilter 
  implements IBuiltInExtensionPoint {

    private IConcurrentConnectionManager concurrentConnectionManager;

    private IConfigurationService configurationService;
    
    private IStatisticManager statisticManager;

    private String reservationUriPattern;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(final ServletRequest req, final ServletResponse resp, final FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) req;
        String poolId = httpRequest.getRequestURI();
        String nodeId = StringUtils.trimToNull(req.getParameter(WebConstants.NODE_ID));
        String method = httpRequest.getMethod();

        if (method.equals("HEAD") && matchesUriPattern(normalizeRequestUri(httpRequest), reservationUriPattern)) {
            // I read here:
            // http://java.sun.com/j2se/1.5.0/docs/guide/net/http-keepalive.html
            // that keepalive likes to have a known content length. I also read
            // that HEAD is better if no content is going to be returned.
            resp.setContentLength(0);
            if (!concurrentConnectionManager.reserveConnection(nodeId, poolId, ReservationType.SOFT)) {
                statisticManager.incrementNodesRejected(1);
                sendError(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
            } else {
                buildSuspendIgnoreResponseHeaders(nodeId, resp);
            }
        } else if (concurrentConnectionManager.reserveConnection(nodeId, poolId, ReservationType.HARD)) {
            try {
                buildSuspendIgnoreResponseHeaders(nodeId, resp);
                chain.doFilter(req, resp);
            } finally {
                concurrentConnectionManager.releaseConnection(nodeId, poolId);
            }
        } else {
            statisticManager.incrementNodesRejected(1);
            sendError(resp, HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
    }

    protected void buildSuspendIgnoreResponseHeaders(final String nodeId, final ServletResponse resp) {
        HttpServletResponse httpResponse = (HttpServletResponse) resp;
        ChannelMap suspendIgnoreChannels = configurationService.getSuspendIgnoreChannelLists(nodeId);
        httpResponse.setHeader(WebConstants.SUSPENDED_CHANNELS, suspendIgnoreChannels.getSuspendChannelsAsString());
        httpResponse.setHeader(WebConstants.IGNORED_CHANNELS, suspendIgnoreChannels.getIgnoreChannelsAsString());
    }

    public void setConcurrentConnectionManager(IConcurrentConnectionManager concurrentConnectionManager) {
        this.concurrentConnectionManager = concurrentConnectionManager;
    }

    public void setReservationUriPattern(String reservationUriPattern) {
        this.reservationUriPattern = reservationUriPattern;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }
    
    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }
}