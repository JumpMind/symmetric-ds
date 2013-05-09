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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.Status;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.ProcessInfoKey.ProcessType;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class FileSyncPullUriHandler extends AbstractUriHandler {

    private ISymmetricEngine engine;

    public FileSyncPullUriHandler(ISymmetricEngine engine, IInterceptor... interceptors) {
        super("/filesync/pull/*", engine.getParameterService(), interceptors);
        this.engine = engine;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);

        if (StringUtils.isBlank(nodeId)) {
            ServletUtils.sendError(res, HttpServletResponse.SC_BAD_REQUEST,
                    "Node must be specified");
            return;
        } else {
            log.debug("File sync pull request received from {}", nodeId);
        }

        IOutgoingTransport outgoingTransport = createOutgoingTransport(res.getOutputStream(),
                engine.getConfigurationService().getSuspendIgnoreChannelLists(nodeId));
        ProcessInfo processInfo = engine.getStatisticManager().newProcessInfo(
                new ProcessInfoKey(engine.getNodeService().findIdentityNodeId(), nodeId,
                        ProcessType.FILE_SYNC_PULL_HANDLER));
        try {
            
            res.setContentType("application/zip");
            res.addHeader("Content-Disposition", "attachment; filename=\"file-sync.zip\"");
            
            engine.getFileSyncService().sendFiles(processInfo,
                    engine.getNodeService().findNode(nodeId), outgoingTransport);
            processInfo.setStatus(Status.DONE);
        } catch (RuntimeException ex) {
            processInfo.setStatus(Status.ERROR);
            throw ex;
        }

    }

}
