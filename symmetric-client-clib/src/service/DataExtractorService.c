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

static unsigned short SymDataExtractorService_batchProcessed(SymOutgoingBatch *batch, SymDataExtractorInfo *info) {
    info->processedBatches->add(info->processedBatches, batch);
    if (strcmp(batch->status, SYM_OUTGOING_BATCH_OK) != 0) {
        batch->loadCount++;
        if (strcmp(batch->status, SYM_OUTGOING_BATCH_IGNORED) != 0) {
            batch->status = SYM_OUTGOING_BATCH_LOADING;
        }
        info->outgoingBatchService->updateOutgoingBatch(info->outgoingBatchService, batch);

        info->bytesSentCount += batch->byteCount;
        info->batchesSentCount++;

        if (info->bytesSentCount >= info->maxBytesToSync && info->processedBatches->size < info->batches->batches->size) {
            SymLog_info("Reached the total byte threshold after %d of %d batches were extracted for node '%s'.  The remaining batches will be extracted on a subsequent sync",
                    info->batchesSentCount, info->batches->batches->size, info->targetNode->nodeId);
            return 0;
        }
    }
    return 1;
}

SymList * SymDataExtractorService_extract(SymDataExtractorService *this, SymNode *targetNode, SymOutgoingTransport *transport) {
    SymList *processedBatches = SymList_new(NULL);
    SymOutgoingBatches *batches = this->outgoingBatchService->getOutgoingBatches(this->outgoingBatchService, targetNode->nodeId);

    if (batches->containsBatches(batches)) {
        // TODO: filter for local and remote suspended channels
        // TODO: update ignored batches

        SymDataExtractorInfo info;
        info.bytesSentCount = 0;
        info.batchesSentCount = 0;
        info.maxBytesToSync = this->parameterService->getLong(this->parameterService, SYM_PARAMETER_TRANSPORT_MAX_BYTES_TO_SYNC, 1048576);
        info.sourceNode = this->nodeService->findIdentity(this->nodeService);
        info.targetNode = targetNode;
        info.outgoingBatchService = this->outgoingBatchService;
        info.processedBatches = processedBatches;
        info.batches = batches;

        SymDataReader *reader = (SymDataReader *) SymExtractDataReader_new(NULL, batches->batches, info.sourceNode->nodeId, targetNode->nodeId,
                this->dataService, this->triggerRouterService, this->platform, (void *) SymDataExtractorService_batchProcessed, (void *) &info);
        SymDataProcessor *processor = (SymDataProcessor *) SymProtocolDataWriter_new(NULL, info.sourceNode->nodeId, reader);

        long rc = transport->process(transport, processor, NULL);
        SymLog_debug("Transport rc = %ld" , rc);

        reader->destroy(reader);
        processor->destroy(processor);
    }
    batches->destroy(batches);
	return processedBatches;
}

void SymDataExtractorService_destroy(SymDataExtractorService *this) {
    free(this);
}

SymDataExtractorService * SymDataExtractorService_new(SymDataExtractorService *this, SymNodeService *nodeService, SymOutgoingBatchService *outgoingBatchService,
        SymDataService *dataService, SymTriggerRouterService *triggerRouterService, SymParameterService *parameterService, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymDataExtractorService *) calloc(1, sizeof(SymDataExtractorService));
    }
    this->nodeService = nodeService;
    this->outgoingBatchService = outgoingBatchService;
    this->dataService = dataService;
    this->parameterService = parameterService;
    this->triggerRouterService = triggerRouterService;
    this->platform = platform;
    this->extract = (void *) &SymDataExtractorService_extract;
    this->destroy = (void *) &SymDataExtractorService_destroy;
    return this;
}
