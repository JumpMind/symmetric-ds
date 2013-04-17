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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.transport.handler.PullResourceHandler;

/**
 * ,
 */
public class PullServlet extends AbstractTransportResourceServlet<PullResourceHandler> 
    implements IBuiltInExtensionPoint {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        // request has the "other" nodes info
        String nodeId = getParameter(req, WebConstants.NODE_ID);

        log.debug("ServletPulling", nodeId);

        if (StringUtils.isBlank(nodeId)) {
            sendError(resp, HttpServletResponse.SC_BAD_REQUEST, "Node must be specified");
            return;
        }

        ChannelMap map = new ChannelMap();
        map.addSuspendChannels(req.getHeader(WebConstants.SUSPENDED_CHANNELS));
        map.addIgnoreChannels(req.getHeader(WebConstants.IGNORED_CHANNELS));

        OutputStream outputStream = createOutputStream(resp);
        // pull out headers and pass to pull() method

        getTransportResourceHandler().pull(nodeId, req.getRemoteHost(), req.getRemoteAddr(), outputStream, map);

        log.debug("ServletPulled", nodeId);

    }

}