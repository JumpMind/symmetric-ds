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

import static org.jumpmind.symmetric.web.WebConstants.MAKE_RESERVATION_PATH;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager.ReservationType;

public class MakeReservationUriHandler extends AbstractUriHandler {

    ISymmetricEngine engine;

    public MakeReservationUriHandler(ISymmetricEngine engine, IInterceptor... interceptors) {
        super(String.format("%s/*", MAKE_RESERVATION_PATH), engine.getParameterService(), interceptors);
        this.engine = engine;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
        String poolId = req.getRequestURI().replace("/" + MAKE_RESERVATION_PATH, "");
        String nodeId = getNodeId(req);
        String channelId = getChannelId(req);
        res.setContentLength(0);

        if (isChannelEnabled(channelId, nodeId)) {
            if (!engine.getConcurrentConnectionManager().reserveConnection(nodeId, channelId, poolId, ReservationType.SOFT)) {
                engine.getStatisticManager().incrementNodesRejected(1);
                ServletUtils.sendError(res, WebConstants.SC_SERVICE_BUSY);
            }
        } else {
            ServletUtils.sendError(res, WebConstants.SC_CHANNEL_DISABLED);
        }
    }
    
    protected boolean isChannelEnabled(String channelId, String nodeId) {
        NodeChannel nodeChannel = engine.getConfigurationService().getNodeChannel(channelId, false);
        boolean enabled = nodeChannel == null || 
                (!nodeChannel.isSuspendEnabled() && !nodeChannel.isIgnoreEnabled() && nodeChannel.isEnabled());
        if (enabled) {
            nodeChannel = engine.getConfigurationService().getNodeChannel(channelId, nodeId, false);
            enabled = nodeChannel == null || 
                    (!nodeChannel.isSuspendEnabled() && !nodeChannel.isIgnoreEnabled() && nodeChannel.isEnabled());
        }
        return enabled;
    }
    
    protected void buildSuspendIgnoreResponseHeaders(final String nodeId, final ServletResponse resp) {
        HttpServletResponse httpResponse = (HttpServletResponse) resp;
        ChannelMap suspendIgnoreChannels = engine.getConfigurationService()
                .getSuspendIgnoreChannelLists(nodeId);
        httpResponse.setHeader(WebConstants.SUSPENDED_CHANNELS,
                suspendIgnoreChannels.getSuspendChannelsAsString());
        httpResponse.setHeader(WebConstants.IGNORED_CHANNELS,
                suspendIgnoreChannels.getIgnoreChannelsAsString());
    }


}
