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
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.transport.handler.AuthenticationResourceHandler;
import org.jumpmind.symmetric.transport.handler.AuthenticationResourceHandler.AuthenticationStatus;

/**
 * This better be the first filter that executes!
 */
public class AuthenticationFilter extends AbstractTransportFilter<AuthenticationResourceHandler> 
  implements IBuiltInExtensionPoint {
    private static final ILog log = LogFactory.getLog(AuthenticationFilter.class);

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException,
            ServletException {
        String securityToken = req.getParameter(WebConstants.SECURITY_TOKEN);
        String nodeId = req.getParameter(WebConstants.NODE_ID);

        if (StringUtils.isEmpty(securityToken) || StringUtils.isEmpty(nodeId)) {
            sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        final AuthenticationStatus status = getTransportResourceHandler().status(nodeId, securityToken);
        if (AuthenticationStatus.FORBIDDEN.equals(status)) {
            sendError(resp, HttpServletResponse.SC_FORBIDDEN);
        } else if (AuthenticationStatus.REGISTRATION_REQUIRED.equals(status)) {
            sendError(resp, WebConstants.REGISTRATION_REQUIRED);
        } else if (AuthenticationStatus.SYNC_DISABLED.equals(status)) {
            sendError(resp, WebConstants.SYNC_DISABLED);
        } else if (AuthenticationStatus.ACCEPTED.equals(status)) {
            chain.doFilter(req, resp);
        }
    }

    @Override
    protected ILog getLog() {
        return log;
    }
}