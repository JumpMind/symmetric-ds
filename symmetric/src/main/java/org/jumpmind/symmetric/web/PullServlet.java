package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;
import org.jumpmind.symmetric.transport.internal.InternalOutgoingTransport;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * The default download rate is 20k/sec. This change be changed via the servlet
 * param <code>kbs-rate</code>
 * 
 * @author awilcox
 *
 */
public class PullServlet extends HttpServlet {

    private static final Log logger = LogFactory.getLog(PullServlet.class);

    private static final long serialVersionUID = 1L;

    @Override
    public void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (req.getParameter(WebConstants.NODE_ID) == null
                || req.getParameter(WebConstants.NODE_ID).trim().length() == 0) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String nodeId = req.getParameter(WebConstants.NODE_ID).trim();

        ApplicationContext ctx = WebApplicationContextUtils
                .getWebApplicationContext(getServletContext());

        IDataExtractorService extractor = (IDataExtractorService) ctx
                .getBean(Constants.DATAEXTRACTOR_SERVICE);
        INodeService nodeService = (INodeService) ctx
                .getBean(Constants.NODE_SERVICE);
        try {
            IOutgoingTransport out = new InternalOutgoingTransport(resp
                    .getOutputStream());
            extractor.extract(nodeService.findNode(nodeId), out);
            out.close();
        } catch (Exception ex) {
            logger.error("Error while pulling data for " + nodeId, ex);
            resp.sendError(501);
        }
    }

}
