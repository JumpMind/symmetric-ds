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
import org.jumpmind.symmetric.transport.metered.MeteredOutputStreamOutgoingTransport;
import org.jumpmind.symmetric.util.MeteredOutputStream;
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

        String clientId = req.getParameter(WebConstants.NODE_ID).trim();

        ApplicationContext ctx = WebApplicationContextUtils
                .getWebApplicationContext(getServletContext());

        IDataExtractorService extractor = (IDataExtractorService) ctx
                .getBean(Constants.DATAEXTRACTOR_SERVICE);
        INodeService clientService = (INodeService) ctx
                .getBean(Constants.NODE_SERVICE);
        try {
            // TODO get the pull rate per client
            String param = getInitParameter("kbs-rate");
            int rate = 20;

            if (param != null) {
                rate = Integer.parseInt(param);
            }

            IOutgoingTransport out = new MeteredOutputStreamOutgoingTransport(
                    resp.getOutputStream(), MeteredOutputStream.KB * rate);
            extractor.extract(clientService.findNode(clientId), out);
            out.close();
        } catch (Exception ex) {
            logger.error("Error while pulling data for " + clientId,ex);
            resp.sendError(501);
        }
    }

}
