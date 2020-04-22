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
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.INodeService.AuthenticationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protect handlers by checking that the request is allowed.
 */
public class AuthenticationInterceptor implements IInterceptor {
    
    private Logger log = LoggerFactory.getLogger(getClass());

    private INodeService nodeService;
    
    private ISecurityService securityService;
    
    private Map<String, AuthenticationSession> sessions = new HashMap<String, AuthenticationSession>();

    private boolean useSessionAuth;
    
    private int sessionExpireMillis;
    
    public AuthenticationInterceptor(INodeService nodeService, ISecurityService securityService, 
            boolean useSessionAuth, int sessionExpireSeconds) {
        this.nodeService = nodeService;
        this.securityService = securityService;
        this.useSessionAuth = useSessionAuth;
        this.sessionExpireMillis = sessionExpireSeconds * 1000;
    }

    public boolean before(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {

        String nodeId = null;
        String securityToken = null;
        AuthenticationStatus status = null;
        AuthenticationSession session = null;

        if (useSessionAuth && (session = getSession(req, false)) != null) {
            nodeId = (String) session.getAttribute(WebConstants.NODE_ID);
            securityToken = (String) session.getAttribute(WebConstants.SECURITY_TOKEN);

            if (!StringUtils.isEmpty(securityToken) && !StringUtils.isEmpty(nodeId)) {
                status = nodeService.getAuthenticationStatus(nodeId, securityToken);
            }
            
            if (status == null || AuthenticationStatus.FORBIDDEN.equals(status) || (AuthenticationStatus.ACCEPTED.equals(status)
                    && sessionExpireMillis > 0 && System.currentTimeMillis() - session.getCreationTime() > sessionExpireMillis)) {
                log.debug("Node '{}' needs to renew authentication", nodeId);
                sessions.remove(session.getId());
                ServletUtils.sendError(resp, WebConstants.SC_AUTH_EXPIRED);
                return false;
            }
        } else {
            securityToken = req.getHeader(WebConstants.HEADER_SECURITY_TOKEN);
            if (securityToken == null) {
                securityToken = req.getParameter(WebConstants.SECURITY_TOKEN);
            }
            nodeId = req.getParameter(WebConstants.NODE_ID);
    
            if (StringUtils.isEmpty(securityToken) || StringUtils.isEmpty(nodeId)) {
                ServletUtils.sendError(resp, WebConstants.SC_FORBIDDEN);
                return false;
            }
    
            status = nodeService.getAuthenticationStatus(nodeId, securityToken);
            
            if (useSessionAuth && AuthenticationStatus.ACCEPTED.equals(status)) {
                session = getSession(req, true);
                session.setAttribute(WebConstants.NODE_ID, nodeId);
                session.setAttribute(WebConstants.SECURITY_TOKEN, securityToken);
                resp.setHeader(WebConstants.HEADER_SET_SESSION_ID, session.getId());
            }
        }
        
        if (AuthenticationStatus.ACCEPTED.equals(status)) {
            log.debug("Node '{}' successfully authenticated", nodeId);
            nodeService.resetNodeFailedLogins(nodeId);
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
            if (AuthenticationStatus.LOCKED.equals(status)) {
                log.warn("Node '{}' failed to authenticate.  It had too many login attempts", nodeId);
            } else {
                log.warn("Node '{}' failed to authenticate.  It had the wrong password", nodeId);
                nodeService.incrementNodeFailedLogins(nodeId);
            }
            ServletUtils.sendError(resp, WebConstants.SC_FORBIDDEN);
            return false;
        }
    }
 
    protected AuthenticationSession getSession(HttpServletRequest req, boolean create) {
        String sessionId = req.getHeader(WebConstants.HEADER_SESSION_ID);
        AuthenticationSession session = sessions.get(sessionId);
        if (session == null && create) {
            String id = securityService.nextSecureHexString(30);
            session = new AuthenticationSession(id);
            sessions.put(id, session);
        }
        return session;
    }

    public void after(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
    }

}