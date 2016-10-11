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

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.reader.DataReaderStatistics;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.util.Statistics;

public class IncomingBatch extends AbstractBatch {

    private static final long serialVersionUID = 1L;

    public enum Status {
       OK("Ok"), ER("Error"), LD("Loading"), RS("Resend"), IG("Ignored"), XX("Unknown");

        private String description;

        Status(String desc) {
            this.description = desc;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private Status status;

    private long databaseMillis;

    private long statementCount;

    private long fallbackInsertCount;

    private long fallbackUpdateCount;

    private long ignoreRowCount;
    
    private long missingDeleteCount;

    private long skipCount;

    private long failedRowNumber;
    
    private long failedLineNumber;    

    private boolean retry;

    public IncomingBatch() {
    }

    public IncomingBatch(Batch batch) {
        setBatchId(batch.getBatchId());
        setNodeId(batch.getSourceNodeId());
        setChannelId(batch.getChannelId());
        this.status = Status.LD;
    }

    public void setValues(Statistics readerStatistics, Statistics writerStatistics,
            boolean isSuccess) {
        if (readerStatistics != null) {
            setByteCount(readerStatistics.get(DataReaderStatistics.READ_BYTE_COUNT));
        }
        if (writerStatistics != null) {
            setFilterMillis(writerStatistics.get(DataWriterStatisticConstants.FILTERMILLIS));
            databaseMillis = writerStatistics.get(DataWriterStatisticConstants.DATABASEMILLIS);
            statementCount = writerStatistics.get(DataWriterStatisticConstants.STATEMENTCOUNT);
            fallbackInsertCount = writerStatistics
                    .get(DataWriterStatisticConstants.FALLBACKINSERTCOUNT);
            fallbackUpdateCount = writerStatistics
                    .get(DataWriterStatisticConstants.FALLBACKUPDATECOUNT);
            missingDeleteCount = writerStatistics
                    .get(DataWriterStatisticConstants.MISSINGDELETECOUNT);
            setIgnoreCount(writerStatistics.get(DataWriterStatisticConstants.IGNORECOUNT));
            ignoreRowCount = writerStatistics.get(DataWriterStatisticConstants.IGNOREROWCOUNT);
            setLastUpdatedTime(new Date());
            if (!isSuccess) {
                failedRowNumber = statementCount;
                failedLineNumber = writerStatistics.get(DataWriterStatisticConstants.LINENUMBER);
            }
        }
    }
    
    public void setNodeBatchId(String value) {
        if (value != null) {
            int splitIndex = value.indexOf("-");
            if (splitIndex > 0) {
                setNodeId(value.substring(0, splitIndex));
                setBatchId(Long.parseLong(value.substring(splitIndex+1)));
            }
        }
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setStatus(String status) {
        try {
            this.status = Status.valueOf(status);
        } catch (IllegalArgumentException e) {
            this.status = Status.XX;
        }
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean isRetry) {
        this.retry = isRetry;
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

    /**
     * An indicator to the incoming batch service as to whether this batch
     * should be saved off.
     * 
     * @return
     */
    public boolean isPersistable() {
        return getBatchId() >= 0;
    }

    public void setFailedLineNumber(long failedLineNumber) {
        this.failedLineNumber = failedLineNumber;
    }
    
    public long getFailedLineNumber() {
        return failedLineNumber;
    }
    
    public long getIgnoreRowCount() {
		return ignoreRowCount;
	}

    public void incrementIgnoreRowCount() {
        this.ignoreRowCount++;
    }
    
	public void setIgnoreRowCount(long ignoreRowCount) {
		this.ignoreRowCount = ignoreRowCount;
	}

    @Override
    public String toString() {
        return Long.toString(getBatchId());
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof IncomingBatch)) {
            return false;
        }
        IncomingBatch b = (IncomingBatch) o;
        return getBatchId() == b.getBatchId() && StringUtils.equals(getNodeId(), b.getNodeId());
    }
    
    @Override
    public int hashCode() {
        return (String.valueOf(getBatchId()) + "-" + getNodeId()).hashCode();
    }
}