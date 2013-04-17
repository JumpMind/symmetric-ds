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

package org.jumpmind.symmetric.service;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.List;

import org.jumpmind.symmetric.extract.DataExtractorContext;
import org.jumpmind.symmetric.extract.IExtractorFilter;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.transport.IOutgoingTransport;

/**
 * This service provides an API to extract and stream data from a source database.
 */
public interface IDataExtractorService {

    public void extractConfiguration(Node node, Writer writer, DataExtractorContext ctx, String... tablesToExclude) throws IOException;
    
    public void extractConfigurationStandalone(Node node, OutputStream out, String... tablesToExclude) throws IOException;

    public void extractConfigurationStandalone(Node node, Writer out, String... tablesToExclude) throws IOException;
    
    public void extractInitialLoadWithinBatchFor(Node node, TriggerRouter trigger, Writer writer,
            DataExtractorContext ctx, TriggerHistory triggerHistory);

    /**
     * @return a list of batches that were extracted
     */
    public List<OutgoingBatch> extract(Node node, IOutgoingTransport transport) throws IOException;

    public boolean extractBatchRange(IOutgoingTransport transport, String startBatchId, String endBatchId)
            throws IOException;

    public boolean extractBatchRange(IExtractListener handler, String startBatchId, String endBatchId) throws IOException;

    public void addExtractorFilter(IExtractorFilter extractorFilter);

}