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
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.IncomingBatch;

public class AckStatusUriHandler extends AbstractUriHandler {

    ISymmetricEngine engine;

    public AckStatusUriHandler(ISymmetricEngine engine, IInterceptor... interceptors) {
        super("/ackstatus/*", engine.getParameterService(), interceptors);
        this.engine = engine;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
        String path = req.getPathInfo();
        if (!StringUtils.isBlank(path)) {
            int batchIdStartIndex = path.lastIndexOf("/") + 1;
            Long batchId = new Long(path.substring(batchIdStartIndex));
            String nodeId = req.getParameter(WebConstants.NODE_ID);
            IncomingBatch batch = engine.getIncomingBatchService().findIncomingBatch(batchId, nodeId);
            if (batch != null) {
                PrintWriter resWriter = res.getWriter();
                resWriter.write(engine.getTransportManager().getAcknowledgementData(false, engine.getNodeService().findIdentityNodeId(),
                        batch));
                resWriter.write("\n");
                resWriter.flush();
                return;
            }
        }
        res.sendError(HttpServletResponse.SC_NOT_FOUND);

    }

}
