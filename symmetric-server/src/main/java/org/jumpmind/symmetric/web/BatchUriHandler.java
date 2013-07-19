/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
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
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.io.IoConstants;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IParameterService;

public class BatchUriHandler extends AbstractCompressionUriHandler {

    private IDataExtractorService dataExtractorService;

    public BatchUriHandler(IParameterService parameterService,
            IDataExtractorService dataExtractorService) {
        super("/batch/*", parameterService);
        this.dataExtractorService = dataExtractorService;
    }

    public void handleWithCompression(HttpServletRequest req, HttpServletResponse res)
            throws IOException, ServletException {
        res.setContentType("text/plain");
        String path = req.getPathInfo();
        if (!StringUtils.isBlank(path)) {
            int batchIdStartIndex = path.lastIndexOf("/") + 1;
            String nodeIdBatchId = path.substring(batchIdStartIndex);
            if (nodeIdBatchId.contains("-")) {
                int dashIndex = nodeIdBatchId.lastIndexOf("-");
                if (dashIndex > 0) {
                    String nodeId = nodeIdBatchId.substring(0, dashIndex);
                    String batchId = nodeIdBatchId.substring(dashIndex+1, nodeIdBatchId.length());
                    if (!write(batchId, nodeId, res.getOutputStream())) {
                        ServletUtils.sendError(res, HttpServletResponse.SC_NOT_FOUND);
                    } else {
                        res.flushBuffer();
                    }
                } else {
                    ServletUtils.sendError(res, HttpServletResponse.SC_NOT_FOUND);
                }
            } else {
                ServletUtils.sendError(res, HttpServletResponse.SC_NOT_FOUND);
            }
        } else {
            res.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    public boolean write(String batchId, String nodeId, OutputStream os) throws IOException {
        
        return dataExtractorService.extractBatchRange(new OutputStreamWriter(os, IoConstants.ENCODING), nodeId,
                Long.valueOf(batchId), Long.valueOf(batchId));
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

}