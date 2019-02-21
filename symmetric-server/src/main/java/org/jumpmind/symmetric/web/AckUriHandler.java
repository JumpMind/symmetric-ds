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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.AbstractTransportManager;

public class AckUriHandler extends AbstractUriHandler {

    private static final Comparator<BatchAck> BATCH_ID_COMPARATOR = new Comparator<BatchAck>() {
        public int compare(BatchAck batchInfo1, BatchAck batchInfo2) {
            Long batchId1 = batchInfo1.getBatchId();
            Long batchId2 = batchInfo2.getBatchId();
            return batchId1.compareTo(batchId2);
        }
    };

    private IAcknowledgeService acknowledgeService;
    
    public AckUriHandler(IParameterService parameterService, IAcknowledgeService acknowledgeService, IInterceptor...interceptors) {
        super("/ack/*", parameterService, interceptors);
        this.acknowledgeService = acknowledgeService;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException, ServletException {
        if (log.isDebugEnabled()) {
            log.debug("Reading ack: {}", req.getParameterMap());
        }
        List<BatchAck> batches = AbstractTransportManager.readAcknowledgement(req.getParameterMap());
        Collections.sort(batches, BATCH_ID_COMPARATOR);

        res.setHeader("Transfer-Encoding", "chunked");
                
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Callable<Object> callable = new Callable<Object>() {
            public Object call() throws Exception {
                ack(batches);
                return null;
            }            
        };
        FutureTask<Object> task = new FutureTask<Object>(callable);
        executor.execute(task);
        
        long keepAliveMillis = parameterService.getLong(ParameterConstants.DATA_LOADER_SEND_ACK_KEEPALIVE);        
        PrintWriter writer = res.getWriter();
        
        while (true) {
            try {
                task.get(keepAliveMillis, TimeUnit.MILLISECONDS);
                break;
            } catch (TimeoutException ex) {
                try {
                    writer.write("1=1&");
                    writer.flush();
                } catch (Exception e) {
                    log.info("Unable to keep client connection alive.  " + e.getClass().getName() + ": " + e.getMessage());
                    task.cancel(true);
                    break;
                }
            } catch (Throwable e) {
                if (e.getCause() != null) {
                    e = e.getCause();
                }
                log.error("Failed to save acks.  " + e.getClass().getName() + ": " + e.getMessage(), e);
                break;
            }
        }
        
        writer.close();
    }

    protected void ack(List<BatchAck> batches) throws IOException {
        for (BatchAck batchInfo : batches) {
            acknowledgeService.ack(batchInfo);
        }
    }

}
