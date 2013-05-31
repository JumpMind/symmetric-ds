package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;

/**
 * Protect handlers by checking that the request is allowed.
 */
public class AuthenticationInterceptor implements IInterceptor {

    public enum AuthenticationStatus {
        SYNC_DISABLED, REGISTRATION_REQUIRED, FORBIDDEN, ACCEPTED;
    };

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

        AuthenticationStatus status = getAuthenticationStatus(nodeId, securityToken);

        if (AuthenticationStatus.ACCEPTED.equals(status)) {
            return true;
        } else if (AuthenticationStatus.REGISTRATION_REQUIRED.equals(status)) {
            ServletUtils.sendError(resp, WebConstants.REGISTRATION_REQUIRED);
            return false;
        } else if (AuthenticationStatus.SYNC_DISABLED.equals(status)) {
            ServletUtils.sendError(resp, WebConstants.SYNC_DISABLED);
            return false;
        } else {
            ServletUtils.sendError(resp, HttpServletResponse.SC_FORBIDDEN);
            return false;
        }
    }
    
    public void after(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
    }

    protected AuthenticationStatus getAuthenticationStatus(String nodeId, String securityToken) {
        AuthenticationStatus retVal = AuthenticationStatus.ACCEPTED;
        Node node = nodeService.findNode(nodeId);
        if (node == null) {
            retVal = AuthenticationStatus.REGISTRATION_REQUIRED;
        } else if (!syncEnabled(node)) {
            retVal = AuthenticationStatus.SYNC_DISABLED;
        } else if (!nodeService.isNodeAuthorized(nodeId, securityToken)) {
            retVal = AuthenticationStatus.FORBIDDEN;
        }
        return retVal;
    }

    protected boolean syncEnabled(Node node) {
        boolean syncEnabled = false;
        if (node != null) {
            syncEnabled = node.isSyncEnabled();
        }
        return syncEnabled;
    }

}