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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.AbstractTransportManager;

public class AckUriHandler extends AbstractUriHandler {

    private static final Comparator<BatchInfo> BATCH_ID_COMPARATOR = new Comparator<BatchInfo>() {
        public int compare(BatchInfo batchInfo1, BatchInfo batchInfo2) {
            Long batchId1 = batchInfo1.getBatchId();
            Long batchId2 = batchInfo1.getBatchId();
            return batchId1.compareTo(batchId2);
        }
    };

    private IAcknowledgeService acknowledgeService;
    
    public AckUriHandler(
            IParameterService parameterService, IAcknowledgeService acknowledgeService, IInterceptor...interceptors) {
        super("/ack/*", parameterService, interceptors);
        this.acknowledgeService = acknowledgeService;
    }

    public void handle(HttpServletRequest req, HttpServletResponse res) throws IOException,
            ServletException {
        if (log.isDebugEnabled()) {
            log.debug("Reading ack: %s", req.getParameterMap());
        }
        @SuppressWarnings("unchecked")
        List<BatchInfo> batches = AbstractTransportManager.readAcknowledgement(req
                .getParameterMap());
        Collections.sort(batches, BATCH_ID_COMPARATOR);
        ack(batches);
    }

    protected void ack(List<BatchInfo> batches) throws IOException {
        for (BatchInfo batchInfo : batches) {
            acknowledgeService.ack(batchInfo);
        }
    }

}
