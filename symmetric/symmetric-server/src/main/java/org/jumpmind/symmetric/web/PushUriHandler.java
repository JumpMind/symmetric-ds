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
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.log.Log;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;

/**
 * Handles data pushes from nodes.
 */
public class PushUriHandler extends AbstractUriHandler {

    private IDataLoaderService dataLoaderService;

    private IStatisticManager statisticManager;
    
    public PushUriHandler(Log log, IParameterService parameterService, IDataLoaderService dataLoaderService,
            IStatisticManager statisticManager,
            IInterceptor... interceptors) {
        super(log, "/push/*", parameterService, interceptors);
        this.dataLoaderService = dataLoaderService;
        this.statisticManager = statisticManager;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {

        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);
        log.debug("DataPushing", nodeId);
        InputStream inputStream = createInputStream(req);
        OutputStream outputStream = res.getOutputStream();

        push(nodeId, inputStream, outputStream);

        // Not sure if this is necessary, but it's been here and it hasn't hurt
        // anything ...
        res.flushBuffer();
        log.debug("DataPushingCompleted", nodeId);

    }

    protected void push(String sourceNodeId, InputStream inputStream, OutputStream outputStream) throws IOException {
        long ts = System.currentTimeMillis();
        try {
            dataLoaderService.loadDataFromPush(sourceNodeId, inputStream, outputStream);
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