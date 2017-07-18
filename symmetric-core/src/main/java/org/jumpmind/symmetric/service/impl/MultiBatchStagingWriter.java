package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.DataWriterStatisticConstants;
import org.jumpmind.symmetric.io.data.writer.IProtocolDataWriterListener;
import org.jumpmind.symmetric.io.data.writer.StagingDataWriter;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
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

    protected boolean cancelled = false;

    public MultiBatchStagingWriter(DataExtractorService dataExtractorService, ExtractRequest request, String sourceNodeId,
            IStagingManager stagingManager, List<OutgoingBatch> batches, long maxBatchSize, ProcessInfo processInfo) {
        this.dataExtractorService = dataExtractorService;
        this.request = request;
        this.sourceNodeId = sourceNodeId;
        this.maxBatchSize = maxBatchSize;
        this.stagingManager = stagingManager;
        this.batches = new ArrayList<OutgoingBatch>(batches);
        this.finishedBatches = new ArrayList<OutgoingBatch>(batches.size());
        this.processInfo = processInfo;
        this.startTime = this.ts = System.currentTimeMillis();
    }

    @Override
    public void open(DataContext context) {
        this.context = context;
        this.nextBatch();
        long memoryThresholdInBytes = this.dataExtractorService.parameterService.getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
        this.currentDataWriter = buildWriter(memoryThresholdInBytes);
        this.currentDataWriter.open(context);
    }

    protected IDataWriter buildWriter(long memoryThresholdInBytes) {
        return new StagingDataWriter(memoryThresholdInBytes, false, sourceNodeId, Constants.STAGING_CATEGORY_OUTGOING, stagingManager,
                (IProtocolDataWriterListener[]) null);
    }

    @Override
    public void close() {
        while (!cancelled && batches.size() > 0 && table != null) {
            startNewBatch();
            end(this.table);
            end(this.batch, false);
        }
        if (table == null && batch != null) {
            log.debug("Batch {} is empty, it will be ignored.", new Object[] { batch.getNodeBatchId() });
        } 
        closeCurrentDataWriter();
    }

    private void closeCurrentDataWriter() {
        if (this.currentDataWriter != null) {
            Statistics stats = this.currentDataWriter.getStatistics().get(batch);
            this.outgoingBatch.setByteCount(stats.get(DataWriterStatisticConstants.BYTECOUNT));
            this.outgoingBatch.setExtractMillis(System.currentTimeMillis() - batch.getStartTime().getTime());
            this.currentDataWriter.close();
            this.currentDataWriter = null;
            checkSend();
        }
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
            this.dataExtractorService.log.info(
                    "Request {} has been processing for {} seconds.  BATCHES={}, ROWS={}, BYTES={}, RANGE={}-{}, CURRENT={}",
                    request.getRequestId(), (System.currentTimeMillis() - startTime) / 1000, finishedBatches.size(), rowCount, byteCount,
                    request.getStartBatchId(), request.getEndBatchId(), batch.getBatchId());
            ts = System.currentTimeMillis();
        }
    }

    public void checkSend() {
        if (this.dataExtractorService.parameterService.is(ParameterConstants.INITIAL_LOAD_EXTRACT_AND_SEND_WHEN_STAGED, false)
                && this.outgoingBatch.getStatus() != Status.OK) {
            IStagedResource resource = this.dataExtractorService.getStagedResource(outgoingBatch);
            if (resource != null) {
                resource.setState(State.DONE);
            }
            OutgoingBatch batchFromDatabase = this.dataExtractorService.outgoingBatchService.findOutgoingBatch(outgoingBatch.getBatchId(),
                    outgoingBatch.getNodeId());
            if (batchFromDatabase.getIgnoreCount() == 0) {
                this.outgoingBatch.setStatus(Status.NE);
            } else {
                cancelled = true;
                throw new CancellationException();
            }
        }

        this.dataExtractorService.outgoingBatchService.updateOutgoingBatch(this.outgoingBatch);
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
        }
        this.outgoingBatch = this.batches.remove(0);
        this.outgoingBatch.setDataRowCount(0);
        this.outgoingBatch.setDataInsertRowCount(0);
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
        long memoryThresholdInBytes = this.dataExtractorService.parameterService.getLong(ParameterConstants.STREAM_TO_FILE_THRESHOLD);
        this.currentDataWriter = buildWriter(memoryThresholdInBytes);
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