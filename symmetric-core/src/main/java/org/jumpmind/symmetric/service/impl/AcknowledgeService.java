package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;

/**
 * @see IAcknowledgeService
 */
public class AcknowledgeService extends AbstractService implements IAcknowledgeService {

    private IOutgoingBatchService outgoingBatchService;

    private List<IAcknowledgeEventListener> batchEventListeners;

    private IRegistrationService registrationService;

    private IStagingManager stagingManger;

    public AcknowledgeService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IOutgoingBatchService outgoingBatchService,
            IRegistrationService registrationService, IStagingManager stagingManager) {
        super(parameterService, symmetricDialect);
        this.outgoingBatchService = outgoingBatchService;
        this.registrationService = registrationService;
        this.stagingManger = stagingManager;
        setSqlMap(new AcknowledgeServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public void ack(final BatchAck batch) {

        if (batchEventListeners != null) {
            for (IAcknowledgeEventListener batchEventListener : batchEventListeners) {
                batchEventListener.onAcknowledgeEvent(batch);
            }
        }

        if (batch.getBatchId() == Constants.VIRTUAL_BATCH_FOR_REGISTRATION) {
            if (batch.isOk()) {
                registrationService.markNodeAsRegistered(batch.getNodeId());
            }
        } else {
            OutgoingBatch outgoingBatch = outgoingBatchService
                    .findOutgoingBatch(batch.getBatchId(), batch.getNodeId());
            Status status = batch.isOk() ? Status.OK : Status.ER;
            if (outgoingBatch != null) {
                // Allow an outside system/user to indicate that a batch
                // is OK.
                if (outgoingBatch.getStatus() != Status.OK) {
                    outgoingBatch.setStatus(status);
                    outgoingBatch.setErrorFlag(!batch.isOk());
                } else {
                    // clearing the error flag in case the user set the batch
                    // status to OK
                    outgoingBatch.setErrorFlag(false);
                    log.warn("Batch {} was already set to OK.  Not updating to {}.",
                            batch.getBatchId(), status.name());
                }
                if (batch.isIgnored()) {
                    outgoingBatch.incrementIgnoreCount();
                }
                outgoingBatch.setNetworkMillis(batch.getNetworkMillis());
                outgoingBatch.setFilterMillis(batch.getFilterMillis());
                outgoingBatch.setLoadMillis(batch.getDatabaseMillis());
                outgoingBatch.setSqlCode(batch.getSqlCode());
                outgoingBatch.setSqlState(batch.getSqlState());
                outgoingBatch.setSqlMessage(batch.getSqlMessage());

                if (!batch.isOk() && batch.getErrorLine() != 0) {
                    List<Number> ids = sqlTemplate.query(getSql("selectDataIdSql"),
                            new NumberMapper(), outgoingBatch.getBatchId());
                    if (ids.size() >= batch.getErrorLine()) {
                        outgoingBatch.setFailedDataId(ids.get((int) batch.getErrorLine() - 1)
                                .longValue());
                    }
                }

                if (status == Status.ER) {
                    log.error(
                            "Received an error from node {} for batch {}.  Check the outgoing_batch table for more info.",
                            outgoingBatch.getNodeId(), outgoingBatch.getBatchId());
                } else {
                    IStagedResource stagingResource = stagingManger.find(
                            Constants.STAGING_CATEGORY_OUTGOING, outgoingBatch.getNodeId(),
                            outgoingBatch.getBatchId());
                    if (stagingResource != null) {
                        stagingResource.setState(State.DONE);
                    }
                }

                outgoingBatchService.updateOutgoingBatch(outgoingBatch);
            } else {
                log.error("Could not find batch {}-{} to acknowledge as {}", new Object[] {batch.getNodeId(), batch.getBatchId(),
                        status.name()});
            }
        }
    }

    public void addAcknowledgeEventListener(IAcknowledgeEventListener statusChangeListner) {

        if (batchEventListeners == null) {
            batchEventListeners = new ArrayList<IAcknowledgeEventListener>();
        }
        batchEventListeners.add(statusChangeListner);
    }
}
