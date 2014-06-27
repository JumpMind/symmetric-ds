package org.jumpmind.symmetric.web;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.ITransportResourceHandler;

/**
 * This Servlet streams the number of bytes requested by the sampleSize
 * parameter
 * 
 * @see IBandwidthService
 */
public class BandwidthSamplerServlet extends AbstractResourceServlet<ITransportResourceHandler> {

    private static final long serialVersionUID = 1L;

    protected Log logger = LogFactory.getLog(getClass());
    
    IParameterService parameterService;
    
    protected long defaultTestSlowBandwidthDelay = 0;

    @Override
    protected Log getLogger() {
        return logger;
    }

    @Override
    protected void handleGet(HttpServletRequest req, HttpServletResponse resp) throws Exception {
        long testSlowBandwidthDelay = parameterService != null ? parameterService.getLong("test.slow.bandwidth.delay") : defaultTestSlowBandwidthDelay;
        
        long sampleSize = 1000;
        try {
            sampleSize = Long.parseLong(req.getParameter("sampleSize"));
        } catch (Exception ex) {
            logger.warn("Invalid sampleSize provided: " + req.getParameter("sampleSize"));
        }
        
        ServletOutputStream os = resp.getOutputStream();
        for(int i = 0; i < sampleSize; i++) {
            os.write(1);
            if (testSlowBandwidthDelay > 0) {
                Thread.sleep(testSlowBandwidthDelay);
            }
        }        
    }
       
    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }


    public void setDefaultTestSlowBandwidthDelay(long defaultTestSlowBandwidthDelay) {
        this.defaultTestSlowBandwidthDelay = defaultTestSlowBandwidthDelay;
    }
}
