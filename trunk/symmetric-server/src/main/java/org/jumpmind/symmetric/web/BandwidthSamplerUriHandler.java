package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.util.AppUtils;

/**
 * This uri handler streams the number of bytes requested by the sampleSize
 * parameter.
 * 
 * @see IBandwidthService
 */
public class BandwidthSamplerUriHandler extends AbstractUriHandler {

    private static final long serialVersionUID = 1L;

    protected long defaultTestSlowBandwidthDelay = 0;

    public BandwidthSamplerUriHandler(IParameterService parameterService) {
        super("/bandwidth/*", parameterService);
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        long testSlowBandwidthDelay = parameterService != null ? parameterService
                .getLong("test.slow.bandwidth.delay") : defaultTestSlowBandwidthDelay;

        long sampleSize = 1000;
        try {
            sampleSize = Long.parseLong(req.getParameter("sampleSize"));
        } catch (Exception ex) {
            log.warn("Unable to parse sampleSize of {}", req.getParameter("sampleSize"));
        }

        ServletOutputStream os = res.getOutputStream();
        for (int i = 0; i < sampleSize; i++) {
            os.write(1);
            if (testSlowBandwidthDelay > 0) {
                AppUtils.sleep(testSlowBandwidthDelay);
            }
        }
    }

    public void setDefaultTestSlowBandwidthDelay(long defaultTestSlowBandwidthDelay) {
        this.defaultTestSlowBandwidthDelay = defaultTestSlowBandwidthDelay;
    }

}
