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
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.reader.DataReaderStatistics;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriterStatistics;
import org.jumpmind.util.Statistics;

public class IncomingBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        LD("Loading"), OK("Ok"), ER("Error");

        private String description;

        Status(String desc) {
            this.description = desc;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private long batchId;

    private String nodeId;

    private String channelId;

    private Status status;

    private boolean errorFlag;

    private long byteCount;

    private long networkMillis;

    private long filterMillis;

    private long databaseMillis;

    private long statementCount;

    private long fallbackInsertCount;

    private long fallbackUpdateCount;

    private long missingDeleteCount;

    private long skipCount;

    private long failedRowNumber;

    private String sqlState;

    private int sqlCode;

    private String sqlMessage;

    private String lastUpdatedHostName;

    private Date lastUpdatedTime;

    private Date createTime;

    private boolean retry;

    public IncomingBatch() {
    }

    public IncomingBatch(Batch batch) {
        this.batchId = batch.getBatchId();
        this.nodeId = batch.getNodeId();
        this.channelId = batch.getChannelId();
        this.status = Status.LD;
    }

    public void setValues(Statistics readerStatistics, Statistics writerStatistics,
            boolean isSuccess) {
        byteCount = readerStatistics.get(DataReaderStatistics.READ_BYTE_COUNT);
        filterMillis = writerStatistics.get(DatabaseWriterStatistics.FILTERMILLIS);
        databaseMillis = writerStatistics.get(DatabaseWriterStatistics.DATABASEMILLIS);
        statementCount = writerStatistics.get(DatabaseWriterStatistics.STATEMENTCOUNT);
        fallbackInsertCount = writerStatistics.get(DatabaseWriterStatistics.FALLBACKINSERTCOUNT);
        fallbackUpdateCount = writerStatistics.get(DatabaseWriterStatistics.FALLBACKUPDATECOUNT);
        missingDeleteCount = writerStatistics.get(DatabaseWriterStatistics.MISSINGDELETECOUNT);
        lastUpdatedTime = new Date();
        if (!isSuccess) {
            failedRowNumber = statementCount;
        }
    }

    public String getNodeBatchId() {
        return nodeId + "-" + batchId;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean isRetry) {
        this.retry = isRetry;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
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

    public long getDatabaseMillis() {
        return databaseMillis;
    }

    public void setDatabaseMillis(long databaseMillis) {
        this.databaseMillis = databaseMillis;
    }

    public long getStatementCount() {
        return statementCount;
    }

    public void setStatementCount(long statementCount) {
        this.statementCount = statementCount;
    }

    public long getFallbackInsertCount() {
        return fallbackInsertCount;
    }

    public void setFallbackInsertCount(long fallbackInsertCount) {
        this.fallbackInsertCount = fallbackInsertCount;
    }

    public long getFallbackUpdateCount() {
        return fallbackUpdateCount;
    }

    public void setFallbackUpdateCount(long fallbackUpdateCount) {
        this.fallbackUpdateCount = fallbackUpdateCount;
    }

    public long getMissingDeleteCount() {
        return missingDeleteCount;
    }

    public void setMissingDeleteCount(long missingDeleteCount) {
        this.missingDeleteCount = missingDeleteCount;
    }

    public void setSkipCount(long skipCount) {
        this.skipCount = skipCount;
    }

    public long getSkipCount() {
        return skipCount;
    }

    public long getFailedRowNumber() {
        return failedRowNumber;
    }

    public void setFailedRowNumber(long failedRowNumber) {
        this.failedRowNumber = failedRowNumber;
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

    public void setLastUpdatedHostName(String lastUpdateHostName) {
        this.lastUpdatedHostName = lastUpdateHostName;
    }

    public Date getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Date lastUpdateTime) {
        this.lastUpdatedTime = lastUpdateTime;
    }

    /**
     * An indicator to the incoming batch service as to whether this batch
     * should be saved off.
     * 
     * @return
     */
    public boolean isPersistable() {
        return batchId >= 0;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public void setErrorFlag(boolean errorFlag) {
        this.errorFlag = errorFlag;
    }

    public boolean isErrorFlag() {
        return errorFlag;
    }

    @Override
    public String toString() {
        return Long.toString(batchId);
    }

}