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
package org.jumpmind.symmetric.service.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.IProtocolDataWriterListener;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiBatchStagingWriter implements IDataWriter {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final DataExtractorService dataExtractorService;

    protected ExtractRequest request;

    protected long maxBatchSize;

    protected IDataWriter currentDataWriter;

    protected List<OutgoingBatch> batches;

    protected List<OutgoingBatch> finishedBatches;

    protected IStagingManager stagingManager;

    protected String sourceNodeId;

    protected DataContext context;

    protected Table table;

    protected OutgoingBatch outgoingBatch;

    protected Batch batch;

    protected boolean inError = false;

    protected ProcessInfo processInfo;

    protected long startTime, ts, rowCount, byteCount;
    
    protected List<ExtractRequest> childRequests;
    
    protected Map<Long, OutgoingBatch> childBatches;
    
    protected long memoryThresholdInBytes;
    
    protected boolean isRestarted;

    public MultiBatchStagingWriter(DataExtractorService dataExtractorService, ExtractRequest request, List<ExtractRequest> childRequests, String sourceNodeId,
            IStagingManager stagingManager, List<OutgoingBatch> batches, long maxBatchSize, ProcessInfo processInfo, boolean isRestarted) {
        this.dataExtractorService = dataExtractorService;
        this.request = request;
        this.sourceNodeId = sourceNodeId;
        this.maxBatchSize = maxBatchSize;
        this.stagingManager = stagingManager;
        this.batches = new ArrayList<OutgoingBatch>(batches);
        this.finishedBatches = new ArrayList<OutgoingBatch>(batches.size());
        this.processInfo = processInfo;
        this.startTime = this.ts = System.currentTimeMillis();
        this.childRequests = childRequests;
        this.memoryThresholdInBytes = this.dataExtractorService.parameterService.getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
        this.childBatches = new HashMap<Long, OutgoingBatch>();
        this.isRestarted = isRestarted;
    }

    @Override
    public void open(DataContext context) {
        this.context = context;
        this.nextBatch();
        this.currentDataWriter = buildWriter();
        this.currentDataWriter.open(context);        
    }

    protected IDataWriter buildWriter() {
        return new StagingDataWriter(memoryThresholdInBytes, false, sourceNodeId, Constants.STAGING_CATEGORY_OUTGOING, stagingManager,
                (IProtocolDataWriterListener[]) null);            
    }

    @Override
    public void close() {
        while (!inError && batches.size() > 0 && table != null) {
            startNewBatch();
            end(this.table);
            end(this.batch, false);
            log.debug("Batch {} is empty", new Object[] { batch.getNodeBatchId() });
            Statistics stats = closeCurrentDataWriter();
            checkSend(stats);
        }
        closeCurrentDataWriter();
    }

    private Statistics closeCurrentDataWriter() {
        Statistics stats = null;
        if (this.currentDataWriter != null) {
            stats = this.currentDataWriter.getStatistics().get(batch);
            this.outgoingBatch.setByteCount(stats.get(DataWriterStatisticConstants.BYTECOUNT));
            this.outgoingBatch.setExtractMillis(System.currentTimeMillis() - batch.getStartTime().getTime());
            this.currentDataWriter.close();
            this.currentDataWriter = null;
            if (inError) {
                IStagedResource resource = this.dataExtractorService.getStagedResource(outgoingBatch);
                if (resource != null) {
                    resource.delete();
                }
            } else {
                checkSend(stats);
            }
        }
        return stats;
    }

    @Override
    public Map<Batch, Statistics> getStatistics() {
        if (currentDataWriter != null) {
            return currentDataWriter.getStatistics();
        } else {
            return Collections.emptyMap();
        }
    }

    public void start(Batch batch) {
        this.batch = batch;
        if (batch != null) {
            processInfo.setCurrentBatchId(batch.getBatchId());
            processInfo.setCurrentChannelId(batch.getChannelId());
            processInfo.incrementBatchCount();
            processInfo.setCurrentDataCount(0);
        }
        this.currentDataWriter.start(batch);
    }

    public boolean start(Table table) {
        this.table = table;
        if (table != null) {
            processInfo.setCurrentTableName(table.getFullyQualifiedTableName());
        }
        this.currentDataWriter.start(table);
        return true;
    }

    @Override
    public void write(CsvData data) {
        this.outgoingBatch.incrementDataRowCount();
        this.outgoingBatch.incrementDataInsertRowCount();
        this.currentDataWriter.write(data);            
        if (this.outgoingBatch.getDataRowCount() >= maxBatchSize && this.batches.size() > 0) {
            this.currentDataWriter.end(table);
            this.currentDataWriter.end(batch, false);
            this.closeCurrentDataWriter();
            startNewBatch();
        }
        if (System.currentTimeMillis() - ts > 60000) {
            long currentRowCount = rowCount + this.currentDataWriter.getStatistics().get(batch).get(DataWriterStatisticConstants.ROWCOUNT);
            long currentByteCount = byteCount + this.currentDataWriter.getStatistics().get(batch).get(DataWriterStatisticConstants.BYTECOUNT);
            this.dataExtractorService.log.info(
                    "Extract request {} for table {} extracting for {} seconds, {} batches, {} rows, and {} bytes.  Current batch is {} in range {}-{}.",
                    request.getRequestId(), request.getTableName(), (System.currentTimeMillis() - startTime) / 1000, finishedBatches.size() + 1,
                    currentRowCount, currentByteCount, batch.getBatchId(), request.getStartBatchId(), request.getEndBatchId());
            ts = System.currentTimeMillis();
        }
    }

    public void checkSend(Statistics stats) {
        IStagedResource resource = this.dataExtractorService.getStagedResource(outgoingBatch);
        if (resource != null) {
            resource.setState(State.DONE);
        }
        OutgoingBatch batchFromDatabase = this.dataExtractorService.outgoingBatchService.findOutgoingBatch(outgoingBatch.getBatchId(),
                outgoingBatch.getNodeId());

        if (!batchFromDatabase.getStatus().equals(Status.OK) && !batchFromDatabase.getStatus().equals(Status.IG)) {
            this.outgoingBatch.setStatus(Status.NE);
            this.outgoingBatch.setExtractRowCount(this.outgoingBatch.getDataRowCount());
            this.outgoingBatch.setExtractInsertRowCount(this.outgoingBatch.getDataInsertRowCount());
            checkSendChildRequests(batchFromDatabase, resource, stats);
            this.dataExtractorService.outgoingBatchService.updateOutgoingBatch(this.outgoingBatch);
        } else {
            // The user canceled a batch before it tried to load, so they probably canceled all batches.
            log.info("User cancelled batches, so cancelling extract request");
            throw new CancellationException();
        }
    }
    
    protected void checkSendChildRequests(OutgoingBatch parentBatch, IStagedResource parentResource, Statistics stats) {
        if (childRequests != null) {
            long batchIndex = outgoingBatch.getBatchId() - request.getStartBatchId();
            for (ExtractRequest childRequest : childRequests) {
                long childBatchId = childRequest.getStartBatchId() + batchIndex;
                Date startExtractTime = new Date();

                if (parentResource != null) {
                    IStagedResource childResource = stagingManager.create(Constants.STAGING_CATEGORY_OUTGOING, 
                            Batch.getStagedLocation(false, childRequest.getNodeId()), childBatchId);
                    log.debug("About to copy batch {} to batch {}-{}", this.outgoingBatch.getNodeBatchId(), childRequest.getNodeId(), childBatchId);
                    BufferedReader reader = parentResource.getReader();
                    BufferedWriter writer = childResource.getWriter(memoryThresholdInBytes);
                    String line = null;
                    try {
                        while ((line = reader.readLine()) != null) {
                            if (line.startsWith(CsvConstants.BATCH) || line.startsWith(CsvConstants.COMMIT)) {
                                line = line.replace(Long.toString(outgoingBatch.getBatchId()), Long.toString(childBatchId));
                            }
                            writer.write(line);
                            writer.write(System.lineSeparator());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to copy batch " + this.outgoingBatch.getNodeBatchId() + " to batch " +
                                childRequest.getNodeId() + "-" + childBatchId + ": " + e.getMessage());
                    } finally {
                        childResource.close();
                        parentResource.close();
                    }
                    childResource.setState(State.DONE);
                }
                
                OutgoingBatch childBatch = this.dataExtractorService.outgoingBatchService.findOutgoingBatch(childBatchId, childRequest.getNodeId());
                childBatch.setExtractStartTime(startExtractTime);
                childBatch.setExtractMillis(System.currentTimeMillis() - startExtractTime.getTime());
                if (stats != null) {
                    childBatch.setDataRowCount(stats.get(DataWriterStatisticConstants.ROWCOUNT));
                    childBatch.setDataInsertRowCount(stats.get(DataWriterStatisticConstants.INSERTCOUNT));
                    childBatch.setByteCount(stats.get(DataWriterStatisticConstants.BYTECOUNT));
                }

                if (!childBatch.getStatus().equals(Status.OK) && !childBatch.getStatus().equals(Status.IG)) {
                    childBatch.setStatus(Status.NE);
                    this.dataExtractorService.outgoingBatchService.updateOutgoingBatch(childBatch);
                }
                childBatches.put(childBatch.getBatchId(), childBatch);
            }
        }
    }

    @Override
    public void end(Table table) {
        if (this.currentDataWriter != null) {
            this.currentDataWriter.end(table);
            Statistics stats = this.currentDataWriter.getStatistics().get(batch);
            this.outgoingBatch.setByteCount(stats.get(DataWriterStatisticConstants.BYTECOUNT));
            this.outgoingBatch.setExtractMillis(System.currentTimeMillis() - batch.getStartTime().getTime());
        }
    }

    @Override
    public void end(Batch batch, boolean inError) {
        this.inError = inError;
        if (this.currentDataWriter != null) {
            this.currentDataWriter.end(this.batch, inError);
            closeCurrentDataWriter();
        }
    }

    protected void nextBatch() {
        if (this.outgoingBatch != null) {
            this.finishedBatches.add(outgoingBatch);
            rowCount += this.outgoingBatch.getDataRowCount();
            byteCount += this.outgoingBatch.getByteCount();
            dataExtractorService.statisticManager.incrementDataBytesExtracted(this.outgoingBatch.getChannelId(), this.outgoingBatch.getByteCount());
            dataExtractorService.statisticManager.incrementDataExtracted(this.outgoingBatch.getChannelId(), this.outgoingBatch.getDataRowCount());
        }
        this.outgoingBatch = this.batches.remove(0);
        this.outgoingBatch.setDataRowCount(0);
        this.outgoingBatch.setDataInsertRowCount(0);
        this.outgoingBatch.setExtractStartTime(new Date());
        if (this.finishedBatches.size() > 0) {
            this.outgoingBatch.setExtractCount(this.outgoingBatch.getExtractCount() + 1);
        }

        /*
         * Update the last update time so the batch isn't purged prematurely
         */
        for (OutgoingBatch batch : finishedBatches) {
            IStagedResource resource = this.dataExtractorService.getStagedResource(batch);
            if (resource != null) {
                resource.refreshLastUpdateTime();
            }
        }
    }

    protected void startNewBatch() {
        this.nextBatch();
        this.currentDataWriter = buildWriter();
        this.batch = new Batch(BatchType.EXTRACT, outgoingBatch.getBatchId(), outgoingBatch.getChannelId(),
                this.dataExtractorService.symmetricDialect.getBinaryEncoding(), sourceNodeId, outgoingBatch.getNodeId(), false);
        this.currentDataWriter.open(context);
        this.currentDataWriter.start(batch);
        processInfo.incrementBatchCount();

        if (table == null) {
            throw new SymmetricException(
                    "'table' cannot null while starting new batch.  Batch: " + outgoingBatch + ". Check trigger/router configs.");
        }
        this.currentDataWriter.start(table);
    }

}