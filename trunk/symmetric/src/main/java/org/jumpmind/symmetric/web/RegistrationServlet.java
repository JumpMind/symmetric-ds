package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

public class RegistrationServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;
    
    protected static final Log logger = LogFactory.getLog(RegistrationServlet.class);

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
            IOException {
        ApplicationContext ctx = WebApplicationContextUtils.getWebApplicationContext(getServletContext());
        IRegistrationService service = (IRegistrationService) ctx.getBean(Constants.REGISTRATION_SERVICE);
        Node node = new Node();
        node.setNodeGroupId(req.getParameter(WebConstants.NODE_GROUP_ID));
        node.setSymmetricVersion(req.getParameter(WebConstants.SYMMETRIC_VERSION));
        node.setExternalId(req.getParameter(WebConstants.EXTERNAL_ID));
        String syncUrlString = req.getParameter(WebConstants.SYNC_URL);
        if (syncUrlString != null && ! syncUrlString.trim().equals("")) {
            node.setSyncURL(syncUrlString);
        }
        node.setSchemaVersion(req.getParameter(WebConstants.SCHEMA_VERSION));
        node.setDatabaseType(req.getParameter(WebConstants.DATABASE_TYPE));
        node.setDatabaseVersion(req.getParameter(WebConstants.DATABASE_VERSION));
        if (!service.registerNode(node, resp.getOutputStream())) {
            logger.warn(node + " was not allowed to register.");
            resp.sendError(WebConstants.REGISTRATION_NOT_OPEN);
        }
    }

}
