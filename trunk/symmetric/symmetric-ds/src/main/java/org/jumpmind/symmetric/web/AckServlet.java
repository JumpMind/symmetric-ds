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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.transport.AbstractTransportManager;
import org.jumpmind.symmetric.transport.handler.AckResourceHandler;

/**
 * @author Chris Henson <chenson42@users.sourceforge.net>
 *
 * @author Keith Naas <knaas@users.sourceforge.net>
 *
 * @author Eric Long <erilong@user.sourceforge.net>
 */
public class AckServlet extends AbstractTransportResourceServlet<AckResourceHandler> implements IBuiltInExtensionPoint {

    private static final BatchIdComparator BATCH_ID_COMPARATOR = new BatchIdComparator();

    private static final long serialVersionUID = 1L;

    @Override
    public boolean isContainerCompatible() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        AckResourceHandler ackService = getTransportResourceHandler();
        log.debug("DataAckReading", req.getParameterMap());
        // TODO: fix this; the servlets need to participate in the transport API
        List<BatchInfo> batches = AbstractTransportManager.readAcknowledgement(req.getParameterMap());
        Collections.sort(batches, BATCH_ID_COMPARATOR);
        ackService.ack(batches);
    }

    private static class BatchIdComparator implements Comparator<BatchInfo> {
        public int compare(BatchInfo batchInfo1, BatchInfo batchInfo2) {
            Long batchId1 = batchInfo1.getBatchId();
            Long batchId2 = batchInfo1.getBatchId();
            return batchId1.compareTo(batchId2);
        }
    }

}