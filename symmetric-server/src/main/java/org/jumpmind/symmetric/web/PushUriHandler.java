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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.impl.DataLoaderService;
import org.jumpmind.symmetric.service.impl.DataLoaderService.DataLoaderWorker;

/**
 * Handles data pushes from nodes.
 */
public class PushUriHandler extends AbstractUriHandler {

    ISymmetricEngine engine;

    public PushUriHandler(ISymmetricEngine engine, IInterceptor... interceptors) {
        super("/push/*", engine.getParameterService(), interceptors);
        this.engine = engine;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
        String nodeId = ServletUtils.getParameter(req, WebConstants.NODE_ID);
        Node sourceNode = engine.getNodeService().findNode(nodeId);
        log.info("About to service push request for {}", nodeId);

        IStagingManager stagingManager = engine.getStagingManager();
        IDataLoaderService dataLoaderService = engine.getDataLoaderService();
        INodeService nodeService = engine.getNodeService();

        BufferedReader reader = new BufferedReader(new InputStreamReader(createInputStream(req)));

        // TODO start network process
        long streamToFileThreshold = parameterService.getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
        String line = reader.readLine();
        StringBuilder batchPrefix = new StringBuilder();
        Long batchId = null;
        BufferedWriter writer = null;
        DataLoaderWorker worker = null;
        while (line != null) {
            if (line.startsWith(CsvConstants.BATCH)) {
                batchId = getBatchId(line);
                IStagedResource resource = stagingManager.create(streamToFileThreshold, Constants.STAGING_CATEGORY_INCOMING, nodeId, batchId);
                writer = resource.getWriter();
                writer.write(batchPrefix.toString());
            } else if (line.startsWith(CsvConstants.COMMIT)) { 
                writer.write(line);
                writer.close();
                writer = null;                
                if (worker == null) {
                  worker = dataLoaderService.createDataLoaderWorker(sourceNode);
                }
                worker.queueUpLoad(new IncomingBatch(batchId, nodeId));
                batchId = null;
            }
            
            if (batchId == null) {
                batchPrefix.append(line).append("\n");            
            } else if (writer != null) {
                writer.write(line);
                writer.write("\n");
            }
            
            line = reader.readLine();
        }
        
        Node identityNode = nodeService.getCachedIdentity();
        
        PrintWriter resWriter = res.getWriter();
        if (worker != null) {
            worker.queueUpLoad(new DataLoaderService.EOM());
            while (!worker.isComplete()) {
                String status = "done";
                IncomingBatch batch = worker.waitForNextBatchToComplete();
                if (batch == null) {
                    status = "in progress";
                    batch = worker.getCurrentlyLoading();
                }
                if (batch != null) {
                    ArrayList<IncomingBatch> list = new ArrayList<IncomingBatch>(1);
                    list.add(batch);
                    log.info("sending {} ack ... for {}", status,  batch);
                    // TODO 13 support
                    resWriter.write(engine.getTransportManager().getAcknowledgementData(false, identityNode.getNodeId(), list));
                    resWriter.write("\n");
                    resWriter.flush();
                }
            }
        }

        reader.close();

        res.flushBuffer();
        log.debug("Done servicing push request for {}", nodeId);

    }

    protected Long getBatchId(String line) {
        String[] tokens = CsvUtils.tokenizeCsvData(line);
        if (tokens.length > 1) {
            return new Long(tokens[1]);
        } else {
            return null;
        }
    }

//    protected void push(String sourceNodeId, InputStream inputStream, OutputStream outputStream) throws IOException {
//        long ts = System.currentTimeMillis();
//        try {
//            Node sourceNode = nodeService.findNode(sourceNodeId);
//            dataLoaderService.loadDataFromPush(sourceNode, inputStream, outputStream);
//        } finally {
//            statisticManager.incrementNodesPushed(1);
//            statisticManager.incrementTotalNodesPushedTime(System.currentTimeMillis() - ts);
//        }
//    }

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
