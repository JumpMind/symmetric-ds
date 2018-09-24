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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.INodeService.AuthenticationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protect handlers by checking that the request is allowed.
 */
public class AuthenticationInterceptor implements IInterceptor {
    
    Logger log = LoggerFactory.getLogger(getClass());

    private INodeService nodeService;
    
    public AuthenticationInterceptor(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public boolean before(HttpServletRequest req, HttpServletResponse resp) throws IOException,
            ServletException {
        String securityToken = req.getParameter(WebConstants.SECURITY_TOKEN);
        String nodeId = req.getParameter(WebConstants.NODE_ID);

        if (StringUtils.isEmpty(securityToken) || StringUtils.isEmpty(nodeId)) {
            ServletUtils.sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            return false;
        }

        AuthenticationStatus status = nodeService.getAuthenticationStatus(nodeId, securityToken);

        if (AuthenticationStatus.ACCEPTED.equals(status)) {
            log.debug("Node '{}' successfully authenticated", nodeId);
            return true;
        } else if (AuthenticationStatus.REGISTRATION_REQUIRED.equals(status)) {
            log.debug("Node '{}' failed to authenticate.  It was not registered", nodeId);
            ServletUtils.sendError(resp, WebConstants.REGISTRATION_REQUIRED);
            return false;
        } else if (AuthenticationStatus.SYNC_DISABLED.equals(status)) {
            log.debug("Node '{}' failed to authenticate.  It was not enabled", nodeId);
            ServletUtils.sendError(resp, WebConstants.SYNC_DISABLED);
            return false;
        } else {
            log.warn("Node '{}' failed to authenticate.  It had the wrong password", nodeId);
            ServletUtils.sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            return false;
        }
    }
    
    public void after(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
    }

}