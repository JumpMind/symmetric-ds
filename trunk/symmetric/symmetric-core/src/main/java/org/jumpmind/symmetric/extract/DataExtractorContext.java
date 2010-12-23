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

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.model.OutgoingBatch;

/**
 * Holds the current state of a data extraction
 */
public class DataExtractorContext implements Cloneable {

    private List<String> auditRecordsWritten = new ArrayList<String>();
    private String lastTriggerHistoryId;
    private String lastRouterId;
    private OutgoingBatch batch;
    private IDataExtractor dataExtractor;

    public DataExtractorContext copy(IDataExtractor extractor) {
        this.dataExtractor = extractor;
        DataExtractorContext newVersion;
        try {
            newVersion = (DataExtractorContext) super.clone();
            newVersion.auditRecordsWritten = new ArrayList<String>();
            return newVersion;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getHistoryRecordsWritten() {
        return auditRecordsWritten;
    }

    public void setLastTriggerHistoryId(String tableName) {
        lastTriggerHistoryId = tableName;
    }
    
    public void setLastRouterId(String lastRouterId) {
        this.lastRouterId = lastRouterId;
    }

    public boolean isLastDataFromSameTriggerAndRouter(String currentTriggerHistoryId, String currentRouterId) {
        return currentTriggerHistoryId != null && lastTriggerHistoryId.equals(currentTriggerHistoryId) && currentRouterId != null && currentRouterId.equals(lastRouterId);
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

}