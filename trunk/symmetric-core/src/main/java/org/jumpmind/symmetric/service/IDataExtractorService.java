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
package org.jumpmind.symmetric.service;

import java.io.OutputStream;
import java.io.Writer;
import java.util.Date;
import java.util.List;

import org.jumpmind.symmetric.io.data.writer.StructureDataWriter.PayloadType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchWithPayload;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

/**
 * This service provides an API to extract and stream data from a source database.
 */
public interface IDataExtractorService {

    public void extractConfigurationStandalone(Node node, OutputStream out);

    public void extractConfigurationStandalone(Node node, Writer out, String... tablesToIgnore);

    public List<OutgoingBatchWithPayload> extractToPayload(ProcessInfo processInfo, Node targetNode, PayloadType payloadType, boolean useJdbcTimestampFormat, boolean useUpsertStatements, boolean useDelimiterIdentifiers);
    
    /**
     * @return a list of batches that were extracted
     */
    public List<OutgoingBatch> extract(ProcessInfo processInfo, Node node, IOutgoingTransport transport);    
    
    public boolean extractBatchRange(Writer writer, String nodeId, long startBatchId, long endBatchId);
    
    public boolean extractBatchRange(Writer writer, String nodeId, Date startBatchTime,
            Date endBatchTime, String... channelIds);    
    
    public boolean extractOnlyOutgoingBatch(String nodeId, long batchId, Writer writer);
    
    public RemoteNodeStatuses queueWork(boolean force);
    
    public void requestExtractRequest(String nodeId, TriggerRouter triggerRouter, long startBatchId, long endBatchId);


}