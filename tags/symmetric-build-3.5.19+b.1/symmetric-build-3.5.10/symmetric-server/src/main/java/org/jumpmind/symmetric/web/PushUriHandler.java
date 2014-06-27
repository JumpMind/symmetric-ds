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
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;

/**
 * Handles data pushes from nodes.
 */
public class PushUriHandler extends AbstractUriHandler {

    private IDataLoaderService dataLoaderService;

    private IStatisticManager statisticManager;
    
    private INodeService nodeService;
    
    public PushUriHandler(IParameterService parameterService, IDataLoaderService dataLoaderService,
            IStatisticManager statisticManager, INodeService nodeService,
            IInterceptor... interceptors) {
        super("/push/*", parameterService, interceptors);
        this.dataLoaderService = dataLoaderService;
        this.statisticManager = statisticManager;
        this.nodeService = nodeService;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {

        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);
        log.debug("Push requested for {}", nodeId);
        InputStream inputStream = createInputStream(req);
        OutputStream outputStream = res.getOutputStream();

        push(nodeId, inputStream, outputStream);

        // Not sure if this is necessary, but it's been here and it hasn't hurt
        // anything ...
        res.flushBuffer();
        log.debug("Push completed for {}", nodeId);

    }

    protected void push(String sourceNodeId, InputStream inputStream, OutputStream outputStream) throws IOException {
        long ts = System.currentTimeMillis();
        try {
            Node sourceNode = nodeService.findNode(sourceNodeId);
            dataLoaderService.loadDataFromPush(sourceNode, inputStream, outputStream);
        } finally {
            statisticManager.incrementNodesPushed(1);
            statisticManager.incrementTotalNodesPushedTime(System.currentTimeMillis() - ts);
        }
    }

    protected InputStream createInputStream(HttpServletRequest req) throws IOException {
        InputStream is = null;
        String contentType = req.getHeader("Content-Type");
        boolean useCompression = contentType != null && contentType.equalsIgnoreCase("gzip");
        is = req.getInputStream();
        if (useCompression) {
            is = new GZIPInputStream(is);
        }
        return is;
    }
    
}
