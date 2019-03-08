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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.stage.IStagedResource;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.BatchAck;
import org.jumpmind.symmetric.model.BatchAckResult;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.statistic.RouterStats;
import org.jumpmind.symmetric.transport.IAcknowledgeEventListener;

/**
 * @see IAcknowledgeService
 */
public class AcknowledgeService extends AbstractService implements IAcknowledgeService {

    private ISymmetricEngine engine;

    public AcknowledgeService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.engine = engine;
        setSqlMap(new AcknowledgeServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public BatchAckResult ack(final BatchAck batch) {

        IRegistrationService registrationService = engine.getRegistrationService();
        IOutgoingBatchService outgoingBatchService = engine.getOutgoingBatchService();
        BatchAckResult result = new BatchAckResult(batch);
    	
        for (IAcknowledgeEventListener listener : engine.getExtensionService().getExtensionPointList(IAcknowledgeEventListener.class)) {
            listener.onAcknowledgeEvent(batch);
        }

        if (batch.getBatchId() == Constants.VIRTUAL_BATCH_FOR_REGISTRATION) {
            if (batch.isOk()) {
                registrationService.markNodeAsRegistered(batch.getNodeId());
            }
        } else {
            OutgoingBatch outgoingBatch = outgoingBatchService
                    .findOutgoingBatch(batch.getBatchId(), batch.getNodeId());
            Status status = batch.isResend() ? Status.RS : batch.isOk() ? Status.OK : Status.ER;
            Status oldStatus = null;
            if (outgoingBatch != null && outgoingBatch.getStatus() != Status.RQ) {
                // Allow an outside system/user to indicate that a batch
                // is OK.
                if (outgoingBatch.getStatus() != Status.OK && 
                        outgoingBatch.getStatus() != Status.IG) {
                    outgoingBatch.setStatus(status);
                    outgoingBatch.setErrorFlag(!batch.isOk());
                } else if (outgoingBatch.getStatus() != Status.OK) {
                    // clearing the error flag in case the user set the batch
                    // status to OK
                    oldStatus = outgoingBatch.getStatus();
                    outgoingBatch.setStatus(Status.OK);
                    outgoingBatch.setErrorFlag(false);
                    status = Status.OK;
                    log.info("Batch {} for node {} was set to {}.  Updating the status to OK.",
                            new Object[] { batch.getBatchId(), batch.getNodeId(), oldStatus.name() });
                }
                if (batch.isIgnored()) {
                    outgoingBatch.incrementIgnoreCount();
                }
                outgoingBatch.setNetworkMillis(batch.getNetworkMillis());
                outgoingBatch.setFilterMillis(batch.getFilterMillis());
                outgoingBatch.setLoadMillis(batch.getLoadMillis());
                outgoingBatch.setLoadStartTime(new Date(batch.getStartTime()));
                outgoingBatch.setSqlCode(batch.getSqlCode());
                outgoingBatch.setSqlState(batch.getSqlState());
                outgoingBatch.setSqlMessage(batch.getSqlMessage());
                outgoingBatch.setLoadRowCount(batch.getLoadRowCount());
                outgoingBatch.setLoadInsertRowCount(batch.getLoadInsertRowCount());
                outgoingBatch.setLoadUpdateRowCount(batch.getLoadUpdateRowCount());
                outgoingBatch.setTransformLoadMillis(batch.getTransformLoadMillis());
                outgoingBatch.setLoadDeleteRowCount(batch.getLoadDeleteRowCount());
                outgoingBatch.setFallbackInsertCount(batch.getFallbackInsertCount());
                outgoingBatch.setFallbackUpdateCount(batch.getFallbackUpdateCount());
                outgoingBatch.setIgnoreRowCount(batch.getIgnoreRowCount());
                outgoingBatch.setMissingDeleteCount(batch.getMissingDeleteCount());
                outgoingBatch.setSkipCount(batch.getSkipCount());

                boolean isNewError = false;
                if (!batch.isOk() && batch.getErrorLine() != 0) {
                    if (outgoingBatch.isLoadFlag()) {
                        isNewError = outgoingBatch.getSentCount() == 1;
                    } else {
                        String sql = getSql("selectDataIdSql");
                        if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_SEQUENCE_NOORDER, false)) {
                            sql = getSql("selectDataIdByCreateTimeSql");
                        } else if (parameterService.is(ParameterConstants.ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED, true)) {
                            sql += getSql("orderByDataId");
                        }
    
                        List<Number> ids = sqlTemplateDirty.query(sql, new NumberMapper(), outgoingBatch.getBatchId());
                        if (ids.size() >= batch.getErrorLine()) {
                            long failedDataId = ids.get((int) batch.getErrorLine() - 1).longValue();
                            if (outgoingBatch.getFailedDataId() == 0 || outgoingBatch.getFailedDataId() != failedDataId) {
                                isNewError = true;
                            }
                            outgoingBatch.setFailedDataId(failedDataId);
                        }
                    }
                }

                if (status == Status.ER) {
                    boolean suppressLogError = false;
                    if (isNewError) {
                        engine.getStatisticManager().incrementDataLoadedOutgoingErrors(outgoingBatch.getChannelId(), 1);
                    }
                    if (isNewError && outgoingBatch.getSqlCode() == ErrorConstants.FK_VIOLATION_CODE) {
                        if (!outgoingBatch.isLoadFlag() && parameterService.is(ParameterConstants.AUTO_RESOLVE_FOREIGN_KEY_VIOLATION)) {
                            engine.getDataService().reloadMissingForeignKeyRows(outgoingBatch.getNodeId(), outgoingBatch.getFailedDataId());
                            suppressLogError = true;
                        }
                        if (outgoingBatch.isLoadFlag() && parameterService.is(ParameterConstants.AUTO_RESOLVE_FOREIGN_KEY_VIOLATION_REVERSE_RELOAD)) {
                            suppressLogError = true;
                        }
                    }
                    if (outgoingBatch.getSqlCode() == ErrorConstants.PROTOCOL_VIOLATION_CODE
                            && ErrorConstants.PROTOCOL_VIOLATION_STATE.equals(outgoingBatch.getSqlState())) {
                        if (outgoingBatch.isLoadFlag()) {
                            log.info("The batch {} may be corrupt in staging. Not removing the batch because it was a load batch, but you may need to clear the batch from staging manually.",
                                    outgoingBatch.getNodeBatchId());
                        } else {
                            IStagedResource resource = engine.getStagingManager().find(Constants.STAGING_CATEGORY_OUTGOING,
                                    outgoingBatch.getStagedLocation(), outgoingBatch.getBatchId());
                            if (resource != null) {
                                log.info("The batch {} may be corrupt in staging, so removing it.", outgoingBatch.getNodeBatchId());
                                resource.delete();
                                suppressLogError = isNewError;
                            }
                        }
                    }
                    if (!suppressLogError) {
                        log.error("The outgoing batch {} failed: {}{}", outgoingBatch.getNodeBatchId(),
                                (batch.getSqlCode() != 0 ? "[" + batch.getSqlState() + "," + batch.getSqlCode() + "] " : ""), batch.getSqlMessage());
                        RouterStats routerStats = engine.getStatisticManager().getRouterStatsByBatch(batch.getBatchId());
                        if (routerStats != null) {
                            log.info("Router stats for batch " + outgoingBatch.getBatchId() + ": " + routerStats.toString());
                        }
                    }
                } else if (status == Status.RS) {
                    log.info("The outgoing batch {} received resend request", outgoingBatch.getNodeBatchId());
                }
                
                outgoingBatchService.updateOutgoingBatch(outgoingBatch);
                if (status == Status.OK) {
                    if (!Status.OK.equals(oldStatus)) {
                        if (outgoingBatch.getLoadId() > 0) {
                            engine.getDataExtractorService().updateExtractRequestLoadTime(new Date(), outgoingBatch);
                        }
                        engine.getStatisticManager().incrementDataLoadedOutgoing(outgoingBatch.getChannelId(), outgoingBatch.getLoadRowCount());
                        engine.getStatisticManager().incrementDataBytesLoadedOutgoing(outgoingBatch.getChannelId(), outgoingBatch.getByteCount());
                    }
                    purgeLoadBatchesFromStaging(outgoingBatch);
                    Channel channel = engine.getConfigurationService().getChannel(outgoingBatch.getChannelId());
                    if (channel != null && channel.isFileSyncFlag()){
                        /* Acknowledge the file_sync in case the file needs deleted. */
                        engine.getFileSyncService().acknowledgeFiles(outgoingBatch);
                    }
                    engine.getStatisticManager().removeRouterStatsByBatch(batch.getBatchId());
                }
            } else if (outgoingBatch == null) {
                log.error("Could not find batch {}-{} to acknowledge as {}", new Object[] {batch.getNodeId(), batch.getBatchId(),
                        status.name()});
                result.setOk(false);
            }
        }
        return result;
    }

    protected void purgeLoadBatchesFromStaging(OutgoingBatch outgoingBatch) {
        long threshold = parameterService.getLong(ParameterConstants.INITIAL_LOAD_PURGE_STAGE_IMMEDIATE_THRESHOLD_ROWS);
        if (threshold >= 0 && outgoingBatch.isLoadFlag() && !outgoingBatch.isCommonFlag()) {
            long count = engine.getDataService().getTableReloadStatusRowCount(outgoingBatch.getLoadId());
            if (count > threshold) {
                IStagedResource resource = engine.getStagingManager().find(Constants.STAGING_CATEGORY_OUTGOING,
                        outgoingBatch.getStagedLocation(), outgoingBatch.getBatchId());
                if (resource != null) {
                    resource.delete();
                }
            }
        }
    }

	public List<BatchAckResult> ack(List<BatchAck> batches) {
		
		List<BatchAckResult> results = new ArrayList<BatchAckResult>();
		for (BatchAck batch:batches) {
			results.add(ack(batch));
		}
		return results;
	}
}
