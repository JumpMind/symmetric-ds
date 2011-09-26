/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;

/**
 * This uri handler streams the number of bytes requested by the sampleSize
 * parameter.
 * 
 * @see IBandwidthService
 */
public class BandwidthSamplerUriHandler extends AbstractUriHandler {

    private static final long serialVersionUID = 1L;

    IParameterService parameterService;

    protected long defaultTestSlowBandwidthDelay = 0;

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        long testSlowBandwidthDelay = parameterService != null ? parameterService
                .getLong("test.slow.bandwidth.delay") : defaultTestSlowBandwidthDelay;

        long sampleSize = 1000;
        try {
            sampleSize = Long.parseLong(req.getParameter("sampleSize"));
        } catch (Exception ex) {
            log.warn("BandwidthSampleSizeParsingFailed", req.getParameter("sampleSize"));
        }

        ServletOutputStream os = res.getOutputStream();
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