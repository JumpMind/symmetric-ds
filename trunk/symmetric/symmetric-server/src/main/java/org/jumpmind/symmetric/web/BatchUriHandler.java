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
package org.jumpmind.symmetric.web;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class BatchUriHandler extends AbstractCompressionUriHandler {

    private IDataExtractorService dataExtractorService;

    public void handleWithCompression(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        res.setContentType("text/plain");
        String path = req.getPathInfo();
        if (!StringUtils.isBlank(path)) {
            int batchIdStartIndex = path.lastIndexOf("/")+1;
            String batchId = path.substring(batchIdStartIndex);
            if (!write(batchId, res.getOutputStream())) {
                ServletUtils.sendError(res, HttpServletResponse.SC_NOT_FOUND);
            } else {
                res.flushBuffer();
            }
        } else {
            res.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    public boolean write(String batchId, OutputStream os) throws IOException {
        IOutgoingTransport transport = createOutgoingTransport(os);
        boolean foundBatch = dataExtractorService.extractBatchRange(transport, batchId, batchId);
        transport.close();
        return foundBatch;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

}