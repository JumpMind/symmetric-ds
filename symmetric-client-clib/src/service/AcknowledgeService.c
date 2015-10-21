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
#include "service/AcknowledgeService.h"

SymBatchAckResult * SymAcknowledgeService_ack(SymAcknowledgeService *this, SymBatchAck *batchAck) {
    SymBatchAckResult *result = SymBatchAckResult_newWithBatchAck(NULL, batchAck);

    SymOutgoingBatch *outgoingBatch = this->outgoingBatchService->findOutgoingBatch(this->outgoingBatchService, batchAck->batchId, batchAck->nodeId);
    char *status = batchAck->isOk ? SYM_OUTGOING_BATCH_OK : SYM_OUTGOING_BATCH_ERROR;

    if (outgoingBatch) {
        // Allow an outside system/user to indicate that a batch is OK.
        if (strcmp(outgoingBatch->status, SYM_OUTGOING_BATCH_OK) != 0 &&
                strcmp(outgoingBatch->status, SYM_OUTGOING_BATCH_IGNORED) != 0) {
            outgoingBatch->status = status;
            outgoingBatch->errorFlag = !batchAck->isOk;
        } else {
            // clearing the error flag in case the user set the batch status to OK
            char *oldStatus = outgoingBatch->status;
            outgoingBatch->status = SYM_OUTGOING_BATCH_OK;
            outgoingBatch->errorFlag = 0;
            SymLog_info("Batch %ld for %s was set to %s.  Updating the status to OK", batchAck->batchId, batchAck->nodeId, oldStatus);
        }
        if (batchAck->ignored) {
            outgoingBatch->ignoreCount++;
        }
        outgoingBatch->networkMillis = batchAck->networkMillis;
        outgoingBatch->filterMillis = batchAck->filterMillis;
        outgoingBatch->loadMillis = batchAck->databaseMillis;
        outgoingBatch->sqlCode = batchAck->sqlCode;
        outgoingBatch->sqlState = batchAck->sqlState;
        outgoingBatch->sqlMessage = batchAck->sqlMessage;

        if (!batchAck->isOk && batchAck->errorLine != 0) {
            SymStringArray *args = SymStringArray_new(NULL);
            args->addLong(args, outgoingBatch->batchId);
            int error;
            SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
            SymList *ids = sqlTemplate->query(sqlTemplate, SYM_SQL_SELECT_DATA_ID, args, NULL, &error, (void *) SymStringMapper_mapper);
            if (ids->size >= batchAck->errorLine) {
                outgoingBatch->failedDataId = atol(ids->get(ids, batchAck->errorLine - 1));
            }
        }

        if (strcmp(outgoingBatch->status, SYM_OUTGOING_BATCH_ERROR) == 0) {
            SymLog_error("The outgoing batch %s:%ld failed, %s", outgoingBatch->nodeId, outgoingBatch->batchId, batchAck->sqlMessage);
        }

        this->outgoingBatchService->updateOutgoingBatch(this->outgoingBatchService, outgoingBatch);
    } else {
        SymLog_error("Could not find batch %s-%ld to acknowledge as %s", batchAck->nodeId, batchAck->batchId, status);
        result->isOk = 0;
    }

    return result;
}

void SymAcknowledgeService_destroy(SymAcknowledgeService *this) {
    free(this);
}

SymAcknowledgeService * SymAcknowledgeService_new(SymAcknowledgeService *this, SymOutgoingBatchService *outgoingBatchService, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymAcknowledgeService *) calloc(1, sizeof(SymAcknowledgeService));
    }
    this->outgoingBatchService = outgoingBatchService;
    this->platform = platform;
    this->ack = (void *) &SymAcknowledgeService_ack;
    this->destroy = (void *) &SymAcknowledgeService_destroy;
    return this;
}
