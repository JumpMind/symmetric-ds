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
package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.db.sql.AbstractSqlMap;
import org.jumpmind.db.sql.mapper.NumberMapper;
import org.jumpmind.log.Log;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.BatchInfo;
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

    public AcknowledgeService(Log log, IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IOutgoingBatchService outgoingBatchService,
            IRegistrationService registrationService) {
        super(log, parameterService, symmetricDialect);
        this.outgoingBatchService = outgoingBatchService;
        this.registrationService = registrationService;
    }

    @Override
    protected AbstractSqlMap createSqlMap() {
        return new AcknowledgeServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens());
    }

    public void ack(final BatchInfo batch) {

        if (batchEventListeners != null) {
            for (IAcknowledgeEventListener batchEventListener : batchEventListeners) {
                batchEventListener.onAcknowledgeEvent(batch);
            }
        }

        if (batch.getBatchId() == BatchInfo.VIRTUAL_BATCH_FOR_REGISTRATION) {
            if (batch.isOk()) {
                registrationService.markNodeAsRegistered(batch.getNodeId());
            }
        } else {
            OutgoingBatch outgoingBatch = outgoingBatchService
                    .findOutgoingBatch(batch.getBatchId());
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
                    log.warn("Batch %d was already set to OK.  Not updating to %s.", batch.getBatchId(), status.name());
                }
                outgoingBatch.setNetworkMillis(batch.getNetworkMillis());
                outgoingBatch.setFilterMillis(batch.getFilterMillis());
                outgoingBatch.setLoadMillis(batch.getDatabaseMillis());

                if (!batch.isOk() && batch.getErrorLine() != 0) {
                    List<Number> ids = sqlTemplate.query(getSql("selectDataIdSql"),
                            new NumberMapper(), outgoingBatch.getBatchId());
                    if (ids.size() >= batch.getErrorLine()) {
                        outgoingBatch.setFailedDataId(ids.get((int) batch.getErrorLine() - 1)
                                .longValue());
                    }
                    outgoingBatch.setSqlCode(batch.getSqlCode());
                    outgoingBatch.setSqlState(batch.getSqlState());
                    outgoingBatch.setSqlMessage(batch.getSqlMessage());
                }

                if (status == Status.ER) {
                    log.error("Received an error from node %s for batch %d.  Check the outgoing_batch table for more info.", outgoingBatch.getNodeId(),
                            outgoingBatch.getBatchId());
                }

                outgoingBatchService.updateOutgoingBatch(outgoingBatch);
            } else {
                log.error("Could not find batch %d to acknowledge as %s", batch.getBatchId(), status.name());
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