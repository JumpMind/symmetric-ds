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
#include "io/reader/ExtractDataReader.h"

void SymExtractDataReader_open(SymExtractDataReader *this) {
    this->datas = this->dataService->selectDataFor(this->dataService, this->batch);
    this->iter = this->datas->iterator(this->datas);
}

SymBatch * SymExtractDataReader_nextBatch(SymExtractDataReader *this) {
    return this->batch;
}

SymTable * SymExtractDataReader_nextTable(SymExtractDataReader *this) {
    return this->targetTable;
}

SymCsvData * SymExtractDataReader_nextData(SymExtractDataReader *this) {
    SymData *data = NULL;
    if (this->iter->hasNext(this->iter)) {
        data = (SymData *) this->iter->next(this->iter);
        SymTriggerHistory *triggerHistory = data->triggerHistory;

        if (strcmp(data->eventType, SYM_DATA_EVENT_RELOAD) == 0) {
            // TODO: implement outgoing reload event
        } else {
            SymTrigger *trigger = NULL; //this->triggerRouterService->getTriggerById(this->triggerRouterService, triggerHist->triggerId);
            if (trigger) {
                if (this->lastTriggerHistory == NULL || this->lastTriggerHistory->triggerHistoryId != triggerHistory->triggerHistoryId ||
                        this->lastRouterId == NULL || strcmp(this->lastRouterId, data->routerId) != 0) {
                    // TODO: lookup and order column according to trigger history
                    //this->sourceTable = lookupAndOrderColumnsAccordingToTriggerHistory(routerId, triggerHistory, false, true);
                    //this->targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(routerId, triggerHistory, true, false);
                }
                this->outgoingBatch->dataEventCount++;
            } else {
                SymLog_error("Could not locate a trigger with the id of %s for %s.  It was recorded in the hist table with a hist id of %d",
                        triggerHistory->triggerId, triggerHistory->sourceTableName, triggerHistory->triggerHistoryId);
            }
            this->lastRouterId = data->routerId;
            this->lastTriggerHistory = triggerHistory;
        }
    }
    return data;
}

void SymExtractDataReader_close(SymExtractDataReader *this) {
    this->iter->destroy(this->iter);
    SymList_destroyAll(this->datas, (void *) SymData_destroy);
}

void SymExtractDataReader_destroy(SymExtractDataReader *this) {
    this->batch->destroy(this->batch);
    free(this);
}

SymExtractDataReader * SymExtractDataReader_new(SymExtractDataReader *this, SymOutgoingBatch *outgoingBatch, char *sourceNodeId, char *targetNodeId,
        SymDataService *dataService, SymTriggerRouterService *triggerRouterService) {
    if (this == NULL) {
        this = (SymExtractDataReader *) calloc(1, sizeof(SymExtractDataReader));
    }
    this->dataService = dataService;
    this->triggerRouterService = triggerRouterService;
    this->outgoingBatch = outgoingBatch;
    this->batch = SymBatch_newWithSettings(NULL, outgoingBatch->batchId, outgoingBatch->channelId, sourceNodeId, targetNodeId);

    SymDataReader *super = &this->super;
    super->batchesProcessed = SymList_new(NULL);
    super->open = (void *) &SymExtractDataReader_open;
    super->close = (void *) &SymExtractDataReader_close;
    super->nextBatch = (void *) &SymExtractDataReader_nextBatch;
    super->nextTable = (void *) &SymExtractDataReader_nextTable;
    super->nextData = (void *) &SymExtractDataReader_nextData;
    super->destroy = (void *) &SymExtractDataReader_destroy;
    return this;
}
