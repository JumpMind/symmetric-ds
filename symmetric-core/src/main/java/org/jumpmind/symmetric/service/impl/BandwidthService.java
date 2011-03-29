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
 * under the License.  */

package org.jumpmind.symmetric.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.BandwidthTestResults;
import org.jumpmind.symmetric.transport.http.HttpTransportManager;

/**
 * @see IBandwidthService
 *
 * 
 */
public class BandwidthService extends AbstractService implements IBandwidthService {
    
    private IParameterService parameterService;

    public double getDownloadKbpsFor(String syncUrl, long sampleSize, long maxTestDuration) {
        double downloadSpeed = -1d;
        try {
            BandwidthTestResults bw = getDownloadResultsFor(syncUrl, sampleSize, maxTestDuration);
            downloadSpeed = (int) bw.getKbps();
        } catch (SocketTimeoutException e) {
            log.warn("SocketTimeOut", syncUrl);
        } catch (Exception e) {
            log.error(e);
        }
        return downloadSpeed;

    }
    
    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    protected BandwidthTestResults getDownloadResultsFor(String syncUrl, long sampleSize, long maxTestDuration)
            throws IOException {
        byte[] buffer = new byte[1024];
        InputStream is = null;
        try {
            BandwidthTestResults bw = new BandwidthTestResults();
            URL u = new URL(String.format("%s/bandwidth?sampleSize=%s", syncUrl, sampleSize));
            bw.start();
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            setBasicAuthIfNeeded(conn);
       
            conn.connect();
            is = conn.getInputStream();
            int r;
            while (-1 != (r = is.read(buffer)) && bw.getElapsed() <= maxTestDuration) {
                bw.transmitted(r);
            }
            is.close();
            bw.stop();
            log.info("BandwidthCalculated", syncUrl, bw.getKbps());
            return bw;
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    protected void setBasicAuthIfNeeded(HttpURLConnection conn) {
        if (parameterService != null) {
            HttpTransportManager.setBasicAuthIfNeeded(conn, 
                parameterService.getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_USERNAME),
                parameterService.getString(ParameterConstants.TRANSPORT_HTTP_BASIC_AUTH_PASSWORD));
        }
    }
}