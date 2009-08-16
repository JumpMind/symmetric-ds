package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * This Servlet streams the number of bytes requested by the sampleSize
 * parameter
 * 
 * @see IBandwidthService
 */
public class BandwidthSamplerServlet extends AbstractResourceServlet {

    private static final long serialVersionUID = 1L;

    protected ILog log = LogFactory.getLog(getClass());

    IParameterService parameterService;

    protected long defaultTestSlowBandwidthDelay = 0;

    @Override
    protected ILog getLog() {
        return log;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        long testSlowBandwidthDelay = parameterService != null ? parameterService.getLong("test.slow.bandwidth.delay")
                : defaultTestSlowBandwidthDelay;

        long sampleSize = 1000;
        try {
            sampleSize = Long.parseLong(req.getParameter("sampleSize"));
        } catch (Exception ex) {
            log.warn("BandwidthSampleSizeParsingFailed", req.getParameter("sampleSize"));
        }

        ServletOutputStream os = resp.getOutputStream();
        for (int i = 0; i < sampleSize; i++) {
            os.write(1);
            if (testSlowBandwidthDelay > 0) {
                AppUtils.sleep(testSlowBandwidthDelay);
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
