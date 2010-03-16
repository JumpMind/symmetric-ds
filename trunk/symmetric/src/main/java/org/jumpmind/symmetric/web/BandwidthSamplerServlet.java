/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.web;

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * This Servlet streams the number of bytes requested by the sampleSize
 * parameter
 * 
 * @see IBandwidthService
 */
public class BandwidthSamplerServlet extends AbstractResourceServlet 
  implements IBuiltInExtensionPoint {
    
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
