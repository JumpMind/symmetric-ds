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
package org.jumpmind.symmetric.extract;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.db.BinaryEncoding;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.util.Context;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Holds the current state of a data extraction
 */
public class DataExtractorContext extends Context implements Cloneable {

    private Map<String, String> historyRecordsWritten = new HashMap<String,String>();
    private String lastTriggerHistoryId;
    private String lastRouterId;
    private OutgoingBatch batch;
    private IDataExtractor dataExtractor;
    private ISymmetricDialect symmetricDialect;
    private JdbcTemplate jdbcTemplate;
    private INodeService nodeService;

    public DataExtractorContext copy(IDataExtractor extractor) {
        this.dataExtractor = extractor;
        DataExtractorContext newVersion;
        try {
            newVersion = (DataExtractorContext) super.clone();
            newVersion.historyRecordsWritten = new HashMap<String, String>();
            newVersion.context = new HashMap<String, Object>();
            return newVersion;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
    
    public long getBatchId() {
        return batch != null ? batch.getBatchId() : Constants.UNROUTED_BATCH_ID;
    }
    
    public BinaryEncoding getBinaryEncoding() {
        return symmetricDialect.getBinaryEncoding();
    }
    
    public Map<String, Object> getContextCache() {
       return context;
    }
    
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    public String getSourceNodeId() {
        return nodeService.findIdentityNodeId();
    }
    
    public Collection<String> getHistoryRecordsWritten() {
        return historyRecordsWritten.values();
    }
    
    public void addHistoryRecordWritten(String tableName, String triggerHistoryId) {
        this.historyRecordsWritten.put(tableName, triggerHistoryId);
    }

    public void setLastTriggerHistoryId(String triggerHistoryId) {
        lastTriggerHistoryId = triggerHistoryId;
    }
    
    public void setLastRouterId(String lastRouterId) {
        this.lastRouterId = lastRouterId;
    }

    public boolean isLastDataFromSameTriggerAndRouter(String currentTriggerHistoryId, String currentRouterId) {
        return lastTriggerHistoryId != null && lastTriggerHistoryId.equals(currentTriggerHistoryId) && lastRouterId != null && lastRouterId.equals(currentRouterId);
    }
    
    public OutgoingBatch getBatch() {
        return batch;
    }

    public void setBatch(OutgoingBatch batch) {
        this.batch = batch;
    }

    public IDataExtractor getDataExtractor() {
        return dataExtractor;
    }
    
    public void incrementDataEventCount() {
        if (batch != null) {
            batch.incrementDataEventCount();
        }
    }
    
    public void incrementByteCount(int size) {
        if (batch != null) {
            batch.incrementByteCount(size);
        }
    }

    public void setSymmetricDialect(ISymmetricDialect symmetricDialect) {
        this.symmetricDialect = symmetricDialect;
    }
    
    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }
    
    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
    
}