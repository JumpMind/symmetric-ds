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
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.io.data.Batch;

public class AbstractBatch implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private long batchId = -1;

    private String nodeId;

    private String channelId;
    
    private boolean errorFlag;    
    
    private long routerMillis;

    private long networkMillis;

    private long filterMillis;

    private long loadMillis;    
    
    private long byteCount;  
    
    private long ignoreCount;
    
    private String sqlState;

    private int sqlCode;

    private String sqlMessage;

    private String lastUpdatedHostName;

    private Date lastUpdatedTime;

    private Date createTime;
    
    private String createBy;
    
    private String summary;
    
    private transient Map<String, Integer> tableCounts = new LinkedHashMap<String, Integer>();    
    
    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }
    
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }    

    public String getNodeBatchId() {
        return nodeId + "-" + batchId;
    }
    
    public void setErrorFlag(boolean errorFlag) {
        this.errorFlag = errorFlag;
    }

    public boolean isErrorFlag() {
        return errorFlag;
    }

    public long getRouterMillis() {
        return routerMillis;
    }     
    
    public void setRouterMillis(long routerMillis) {
        this.routerMillis = routerMillis;
    }

    public long getNetworkMillis() {
        return networkMillis;
    }

    public void setNetworkMillis(long networkMillis) {
        this.networkMillis = networkMillis;
    }

    public long getFilterMillis() {
        return filterMillis;
    }

    public void setFilterMillis(long filterMillis) {
        this.filterMillis = filterMillis;
    }

    public long getLoadMillis() {
        return loadMillis;
    }

    public void setLoadMillis(long databaseMillis) {
        this.loadMillis = databaseMillis;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }    
    
    public String getStagedLocation() {
        return Batch.getStagedLocation(false, getNodeId());
    }
    
    public void incrementByteCount(int size) {
        this.byteCount += size;
    }    
    
    public void setIgnoreCount(long ignoreCount) {
        this.ignoreCount = ignoreCount;
    }
    
    public void incrementIgnoreCount() {
        this.ignoreCount++;
    }
    
    public long getIgnoreCount() {
        return ignoreCount;
    }
    
    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public int getSqlCode() {
        return sqlCode;
    }

    public void setSqlCode(int sqlCode) {
        this.sqlCode = sqlCode;
    }

    public String getSqlMessage() {
        return sqlMessage;
    }

    public void setSqlMessage(String sqlMessage) {
        this.sqlMessage = sqlMessage;
    }

    public String getLastUpdatedHostName() {
        return lastUpdatedHostName;
    }

    public void setLastUpdatedHostName(String lastUpdatedHostName) {
        this.lastUpdatedHostName = lastUpdatedHostName;
    }

    public Date getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Date lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
    
    public void setCreateBy(String createBy) {
        this.createBy = createBy;
    }
    
    public String getCreateBy() {
        return createBy;
    }
    
    public String getSummary() {
        if ((summary == null || summary.length() == 0) && tableCounts != null) {
            summary = buildBatchSummary();
        }
        return summary;
    }
    
    public void setSummary(String summary) {
        this.summary = summary;
    }
    
    protected String buildBatchSummary() {
        final int SIZE = 254;
        StringBuilder buff = new StringBuilder(SIZE);
        
        for (Entry<String, Integer> tableCount : tableCounts.entrySet()) {
            buff.append(tableCount.getKey()).append(", ");
        }
        
        if (buff.length() > 2) {            
            buff.setLength(buff.length()-2);
        }
        
        return StringUtils.abbreviate(buff.toString(), SIZE);        
    }
    
    public void incrementTableCount(String tableName) {
        Integer count = tableCounts.get(tableName); 
        if (count == null) {
            count = Integer.valueOf(0);
        }
        tableCounts.put(tableName, ++count);
        summary = null;
    }
}
