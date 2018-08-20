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
import static org.jumpmind.symmetric.io.data.reader.DataReaderStatistics.*;
import static org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants.*;

public class IncomingBatch extends AbstractBatch {

    private static final long serialVersionUID = 1L;

    private long failedRowNumber;

    private long failedLineNumber;

    private long startTime;

    private boolean retry;

    public IncomingBatch() {
    }

    public IncomingBatch(Batch batch) {
        setBatchId(batch.getBatchId());
        setNodeId(batch.getSourceNodeId());
        setChannelId(batch.getChannelId());
        setStatus(Status.LD);
    }

    public void setValues(Statistics readerStatistics, Statistics writerStatistics, boolean isSuccess) {
        if (readerStatistics != null) {
            setByteCount(readerStatistics.get(DataReaderStatistics.READ_BYTE_COUNT));
            mergeInjectedBatchStatistics(readerStatistics);
        }
        if (writerStatistics != null) {
            setFilterMillis(writerStatistics.get(FILTERMILLIS));
            setLoadMillis(writerStatistics.get(LOADMILLIS));
            setLoadRowCount(writerStatistics.get(ROWCOUNT));
            setTransformLoadMillis(writerStatistics.get(TRANSFORMMILLIS));
            setFallbackInsertCount(writerStatistics.get(FALLBACKINSERTCOUNT));
            setFallbackUpdateCount(writerStatistics.get(FALLBACKUPDATECOUNT));
            setMissingDeleteCount(writerStatistics.get(DataWriterStatisticConstants.MISSINGDELETECOUNT));
            setIgnoreCount(writerStatistics.get(DataWriterStatisticConstants.IGNORECOUNT));
            setIgnoreRowCount(writerStatistics.get(DataWriterStatisticConstants.IGNOREROWCOUNT));
            setStartTime(writerStatistics.get(DataWriterStatisticConstants.STARTTIME));
            setLastUpdatedTime(new Date());
            if (!isSuccess) {
                failedRowNumber = getLoadRowCount();
                failedLineNumber = writerStatistics.get(DataWriterStatisticConstants.LINENUMBER);
            }

            setLoadInsertRowCount(writerStatistics.get(DataWriterStatisticConstants.INSERTCOUNT));
            setLoadUpdateRowCount(writerStatistics.get(DataWriterStatisticConstants.UPDATECOUNT));
            setLoadDeleteRowCount(writerStatistics.get(DataWriterStatisticConstants.DELETECOUNT));
        }
    }

    public void mergeInjectedBatchStatistics(Statistics statistics) {
        if (statistics.contains(LOAD_FLAG)) setLoadFlag(statistics.get(LOAD_FLAG) == 1);
        if (statistics.contains(EXTRACT_COUNT)) setExtractCount(statistics.get(DataReaderStatistics.EXTRACT_COUNT));
        if (statistics.contains(SENT_COUNT)) setSentCount(statistics.get(DataReaderStatistics.SENT_COUNT));
        if (statistics.contains(LOAD_COUNT)) setLoadCount(statistics.get(DataReaderStatistics.LOAD_COUNT));
        if (statistics.contains(LOAD_ID)) setLoadId(statistics.get(DataReaderStatistics.LOAD_ID));
        if (statistics.contains(COMMON_FLAG)) setCommonFlag(statistics.get(DataReaderStatistics.COMMON_FLAG) == 1);
        if (statistics.contains(ROUTER_MILLIS)) setRouterMillis(statistics.get(DataReaderStatistics.ROUTER_MILLIS));
        if (statistics.contains(EXTRACT_MILLIS)) setExtractMillis(statistics.get(DataReaderStatistics.EXTRACT_MILLIS));
        if (statistics.contains(TRANSFORM_EXTRACT_MILLIS)) setTransformExtractMillis(statistics.get(DataReaderStatistics.TRANSFORM_EXTRACT_MILLIS));
        if (statistics.contains(TRANSFORM_LOAD_MILLIS)) setTransformLoadMillis(statistics.get(DataReaderStatistics.TRANSFORM_LOAD_MILLIS));
        if (statistics.contains(RELOAD_ROW_COUNT)) setReloadRowCount(statistics.get(DataReaderStatistics.RELOAD_ROW_COUNT));
        if (statistics.contains(OTHER_ROW_COUNT)) setOtherRowCount(statistics.get(DataReaderStatistics.OTHER_ROW_COUNT));
        if (statistics.contains(DATA_ROW_COUNT)) setDataRowCount(statistics.get(DataReaderStatistics.DATA_ROW_COUNT));
        if (statistics.contains(DATA_INSERT_ROW_COUNT)) setDataInsertRowCount(statistics.get(DataReaderStatistics.DATA_INSERT_ROW_COUNT));
        if (statistics.contains(DATA_UPDATE_ROW_COUNT)) setDataUpdateRowCount(statistics.get(DataReaderStatistics.DATA_UPDATE_ROW_COUNT));
        if (statistics.contains(DATA_DELETE_ROW_COUNT)) setDataDeleteRowCount(statistics.get(DataReaderStatistics.DATA_DELETE_ROW_COUNT));
        if (statistics.contains(EXTRACT_ROW_COUNT)) setExtractRowCount(statistics.get(DataReaderStatistics.EXTRACT_ROW_COUNT));
        if (statistics.contains(EXTRACT_INSERT_ROW_COUNT)) setExtractInsertRowCount(statistics.get(DataReaderStatistics.EXTRACT_INSERT_ROW_COUNT));
        if (statistics.contains(EXTRACT_UPDATE_ROW_COUNT)) setExtractUpdateRowCount(statistics.get(DataReaderStatistics.EXTRACT_UPDATE_ROW_COUNT));
        if (statistics.contains(EXTRACT_DELETE_ROW_COUNT)) setExtractDeleteRowCount(statistics.get(DataReaderStatistics.EXTRACT_DELETE_ROW_COUNT));
        if (statistics.contains(FAILED_DATA_ID)) setFailedDataId(statistics.get(DataReaderStatistics.FAILED_DATA_ID));
    }   
        
    public void setNodeBatchId(String value) {
        if (value != null) {
            int splitIndex = value.lastIndexOf("-");
            if (splitIndex > 0) {
                setNodeId(value.substring(0, splitIndex));
                setBatchId(Long.parseLong(value.substring(splitIndex + 1)));
            }
        }
    }

    public boolean isRetry() {
        return retry;
    }

    public void setRetry(boolean isRetry) {
        this.retry = isRetry;
    }

    public long getFailedRowNumber() {
        return failedRowNumber;
    }

    public void setFailedRowNumber(long failedRowNumber) {
        this.failedRowNumber = failedRowNumber;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
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

    @Override
    public String toString() {
        return "IncomingBatch " + getBatchId();
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