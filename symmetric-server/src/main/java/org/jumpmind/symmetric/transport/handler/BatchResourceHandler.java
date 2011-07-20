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
import java.io.OutputStream;

import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

public class BatchResourceHandler extends AbstractTransportResourceHandler {

    private IDataExtractorService dataExtractorService;

    public boolean write(String batchId, OutputStream os) throws IOException {
        IOutgoingTransport transport = createOutgoingTransport(os);
        boolean foundBatch = dataExtractorService.extractBatchRange(transport, batchId, batchId);
        transport.close();
        return foundBatch;
    }

    public void setDataExtractorService(IDataExtractorService dataExtractorService) {
        this.dataExtractorService = dataExtractorService;
    }

}