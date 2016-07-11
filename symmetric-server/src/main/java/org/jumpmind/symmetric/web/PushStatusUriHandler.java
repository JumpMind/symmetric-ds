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
import java.io.InputStream;
import java.io.OutputStream;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileUploadException;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.springframework.util.StringUtils;

public class PushStatusUriHandler extends AbstractUriHandler {
    
    private IDataLoaderService dataLoaderService;

    private IStatisticManager statisticManager;
    
    private INodeService nodeService;
    
    public PushStatusUriHandler(IParameterService parameterService, IDataLoaderService dataLoaderService,
            IStatisticManager statisticManager, INodeService nodeService,
            IInterceptor... interceptors) {
        super("/pushstatus/*", parameterService, interceptors);
        this.dataLoaderService = dataLoaderService;
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
    }

    @Override
    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException, FileUploadException {
        // http://localhost:31415/sync/corp-000/pushstats?nodeId=001&securityToken=5d1c92bbacbe2edb9e1ca5dbb0e481&hostName=mac-mmichalek.local&ipAddress=fe80%3A0%3A0%3A0%3A0%3A0%3A0%3A1%251
        
        
        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);
        String batchToSendCountParam = ServletUtils.getParameter(req, "batch_to_send_count");
        if (!StringUtils.isEmpty(batchToSendCountParam)) {
            try {                
                int batchToSendCount = Integer.parseInt(batchToSendCountParam.trim());
                System.out.println("Batch nodeId: " + nodeId + " to send count " + batchToSendCount);
            } catch (Exception ex) {
                log.warn("Failed to parse batchToSendCountParam [" + batchToSendCountParam + "]", ex);
                res.sendError(HttpServletResponse.SC_BAD_REQUEST, "Couldn't parse batch_to_send_count.");
            }
        }
        log.debug("Push stats requested for {}", nodeId);
//        InputStream inputStream = createInputStream(req);
//        OutputStream outputStream = res.getOutputStream();

    }

}
