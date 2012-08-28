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


package org.jumpmind.symmetric.model;

import java.io.Serializable;

/**
 * Status of a batch acknowledgement
 */
public class BatchAck  implements Serializable {
    
    private static final long serialVersionUID = 1L;

    public static final long VIRTUAL_BATCH_FOR_REGISTRATION = -9999;
    
    private long batchId;

    private String nodeId;

    private boolean isOk;

    private long errorLine;

    private long networkMillis;

    private long filterMillis;

    private long databaseMillis;

    private long byteCount;

    private String sqlState;

    private int sqlCode;
    
    private boolean ignored = false;

    private String sqlMessage;

    public BatchAck(long batchId) {
        this.batchId = batchId;
        isOk = true;
    }

    public BatchAck(long batchId, long errorLineNumber) {
        this.batchId = batchId;
        isOk = false;
        errorLine = errorLineNumber;
    }

    public long getBatchId() {
        return batchId;
    }

    public long getErrorLine() {
        return errorLine;
    }

    public boolean isOk() {
        return isOk;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public void setErrorLine(long errorLine) {
        this.errorLine = errorLine;
    }

    public void setOk(boolean isOk) {
        this.isOk = isOk;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

    public long getDatabaseMillis() {
        return databaseMillis;
    }

    public void setDatabaseMillis(long databaseMillis) {
        this.databaseMillis = databaseMillis;
    }

    public long getFilterMillis() {
        return filterMillis;
    }

    public void setFilterMillis(long filterMillis) {
        this.filterMillis = filterMillis;
    }

    public long getNetworkMillis() {
        return networkMillis;
    }

    public void setNetworkMillis(long networkMillis) {
        this.networkMillis = networkMillis;
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

    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
    
    public void setIgnored(boolean ignored) {
        this.ignored = ignored;
    }
    
    public boolean isIgnored() {
        return ignored;
    }

}