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


package org.jumpmind.symmetric.transport.handler;

import java.io.IOException;
import java.util.List;

import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.service.IAcknowledgeService;

/**
 * ,
 */
public class AckResourceHandler extends AbstractTransportResourceHandler {
    private IAcknowledgeService acknowledgeService;

    public void ack(List<BatchInfo> batches) throws IOException {
        for (BatchInfo batchInfo : batches) {
            acknowledgeService.ack(batchInfo);
        }
    }

    public void setAcknowledgeService(IAcknowledgeService acknowledgeService) {
        this.acknowledgeService = acknowledgeService;
    }
}