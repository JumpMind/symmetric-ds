package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.RegistrationRedirectException;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;

/**
 * Handler that delegates to the {@link IRegistrationService}
 */
public class RegistrationUriHandler extends AbstractUriHandler {
    
    private IRegistrationService registrationService;        
    
    public RegistrationUriHandler( IParameterService parameterService,
            IRegistrationService registrationService, IInterceptor... interceptors) {
        super("/registration/*", parameterService, interceptors);
        this.registrationService = registrationService;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        Node node = transform(req);
        try {
            OutputStream outputStream = res.getOutputStream();
            if (!registerNode(node, getHostName(req), getIpAddress(req), outputStream)) {
                log.warn("{} was not allowed to register.", node);
                ServletUtils.sendError(res, WebConstants.REGISTRATION_NOT_OPEN, String.format("%s was not allowed to register.",
                        node));
            }
        } catch (RegistrationRedirectException e) {
            res.sendRedirect(HttpTransportManager.buildRegistrationUrl(e.getRedirectionUrl(), node));
        }
    }

    private Node transform(HttpServletRequest req) {
        Node node = new Node();
        node.setNodeGroupId(ServletUtils.getParameter(req, WebConstants.NODE_GROUP_ID));
        node.setSymmetricVersion(ServletUtils.getParameter(req, WebConstants.SYMMETRIC_VERSION));
        node.setExternalId(ServletUtils.getParameter(req, WebConstants.EXTERNAL_ID));
        String syncUrlString = ServletUtils.getParameter(req, WebConstants.SYNC_URL);
        if (StringUtils.isNotBlank(syncUrlString)) {
            node.setSyncUrl(syncUrlString);
        }
        node.setSchemaVersion(ServletUtils.getParameter(req, WebConstants.SCHEMA_VERSION));
        node.setDatabaseType(ServletUtils.getParameter(req, WebConstants.DATABASE_TYPE));
        node.setDatabaseVersion(ServletUtils.getParameter(req, WebConstants.DATABASE_VERSION));
        return node;
    }
    
    protected String getHostName(HttpServletRequest req) {
        String hostName = ServletUtils.getParameter(req, WebConstants.HOST_NAME);
        if (StringUtils.isBlank(hostName)) {
            hostName = req.getRemoteHost();
        }
        return hostName;
    }
    
    protected String getIpAddress(HttpServletRequest req) {
        String ipAdddress = ServletUtils.getParameter(req, WebConstants.IP_ADDRESS);
        if (StringUtils.isBlank(ipAdddress)) {
            ipAdddress = req.getRemoteAddr();
        }
        return ipAdddress;
    }

    protected boolean registerNode(Node node, String remoteHost, String remoteAddress, OutputStream outputStream) throws IOException {
        return registrationService.registerNode(node, remoteHost, remoteAddress, outputStream, true);
    }
    
}
