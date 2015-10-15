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
#include "service/DataExtractorService.h"

static SymList * SymDataExtractorService_extractOutgoingBatch(SymDataExtractorService *this, SymNode *sourceNode, SymNode *targetNode,
        SymOutgoingTransport *transport, SymOutgoingBatch *outgoingBatch) {
    SymDataReader *reader = (SymDataReader *) SymExtractDataReader_new(NULL, outgoingBatch, sourceNode->nodeId, targetNode->nodeId);
    SymDataProcessor *processor = (SymDataProcessor *) SymProtocolDataWriter_new(NULL, sourceNode->nodeId, reader);
    long rc = transport->process(transport, processor);
    SymLog_debug("Transport rc = %ld" , rc);
    SymList *batchesProcessed = processor->getBatchesProcessed(processor);
    reader->destroy(reader);
    processor->destroy(processor);
    return batchesProcessed;
}

SymList * SymDataExtractorService_extract(SymDataExtractorService *this, SymNode *targetNode, SymOutgoingTransport *transport) {

    SymList *processedBatches = SymList_new(NULL);
    SymOutgoingBatches *batches = this->outgoingBatchService->getOutgoingBatches(this->outgoingBatchService, targetNode->nodeId);

    if (batches->containsBatches(batches)) {
        // TODO: filter for local and remote suspended channels
        // TODO: update ignored batches

        long bytesSentCount = 0;
        int batchesSentCount = 0;
        long maxBytesToSync = this->parameterService->getLong(this->parameterService, SYM_TRANSPORT_MAX_BYTES_TO_SYNC, 1048576);
        SymNode *nodeIdentity = this->nodeService->findIdentity(this->nodeService);
        SymIterator *iter = batches->batches->iterator(batches->batches);

        while (iter->hasNext(iter)) {
            SymOutgoingBatch *batch = (SymOutgoingBatch *) iter->next(iter);
            SymDataExtractorService_extractOutgoingBatch(this, nodeIdentity, targetNode, transport, batch);

            if (strcmp(batch->status, SYM_OUTGOING_BATCH_OK) == 0) {
                batch->loadCount++;
                if (strcmp(batch->status, SYM_OUTGOING_BATCH_IGNORED) != 0) {
                    batch->status = SYM_OUTGOING_BATCH_LOADING;
                }
                this->outgoingBatchService->updateOutgoingBatch(this->outgoingBatchService, batch);

                bytesSentCount += batch->byteCount;
                batchesSentCount++;

                if (bytesSentCount >= maxBytesToSync && processedBatches->size < batches->batches->size) {
                    SymLog_info("Reached the total byte threshold after %d of %d batches were extracted for node '%s'.  The remaining batches will be extracted on a subsequent sync",
                            batchesSentCount, batches->batches->size, targetNode->nodeId);
                    break;
                }
            }
        }
        iter->destroy(iter);
    }

	return processedBatches;
}

void SymDataExtractorService_destroy(SymDataExtractorService *this) {
    free(this);
}

SymDataExtractorService * SymDataExtractorService_new(SymDataExtractorService *this, SymNodeService *nodeService, SymOutgoingBatchService *outgoingBatchService,
        SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymDataExtractorService *) calloc(1, sizeof(SymDataExtractorService));
    }
    this->nodeService = nodeService;
    this->outgoingBatchService = outgoingBatchService;
    this->parameterService = parameterService;
    this->extract = (void *) &SymDataExtractorService_extract;
    this->destroy = (void *) &SymDataExtractorService_destroy;
    return this;
}
