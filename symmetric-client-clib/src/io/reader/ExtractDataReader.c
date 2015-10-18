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

static SymTable * lookupAndOrderColumnsAccordingToTriggerHistory(SymExtractDataReader *this, char *routerId, SymTriggerHistory *triggerHistory,
        unsigned short setTargetTableName, unsigned short useDatabaseDefinition) {
    char *catalogName = triggerHistory->sourceCatalogName;
    char *schemaName = triggerHistory->sourceSchemaName;
    char *tableName = triggerHistory->sourceTableName;
    SymTable *table = NULL;
    if (useDatabaseDefinition) {
        table = this->platform->getTableFromCache(this->platform, catalogName, schemaName, tableName, 0);

        SymList *parsedColumns = triggerHistory->getParsedColumns(triggerHistory);
        if (table && table->columns->size < parsedColumns->size) {
            /*
             * If the column count is less than what trigger history reports, then
             * chances are the table cache is out of date.
             */
            table = this->platform->getTableFromCache(this->platform, catalogName, schemaName, tableName, 1);
        }

        if (table) {
            table = table->copyAndFilterColumns(table, parsedColumns, 1);
        } else {
            SymLog_error("Could not find the following table.  It might have been dropped: %s",
                    SymTable_getFullyQualifiedTableName(catalogName, schemaName, tableName, "", ".", "."));
            return NULL;
        }
    } else {
        table = SymTable_newWithName(NULL, tableName);
        table->columns = triggerHistory->getParsedColumns(triggerHistory);
    }

    SymRouter *router = this->triggerRouterService->getRouterById(this->triggerRouterService, routerId, 0);
    if (router && setTargetTableName) {
        if (router->useSourceCatalogSchema) {
            table->catalog = catalogName;
            table->schema = schemaName;
        } else {
            table->catalog = NULL;
            table->schema = NULL;
        }

        if (SymStringUtils_equals(SYM_NONE_TOKEN, router->targetCatalogName)) {
            table->catalog = NULL;
        } else if (SymStringUtils_isNotBlank(router->targetCatalogName)) {
            table->catalog = router->targetCatalogName;
        }

        if (SymStringUtils_equals(SYM_NONE_TOKEN, router->targetSchemaName)) {
            table->schema = NULL;
        } else if (SymStringUtils_isNotBlank(router->targetSchemaName)) {
            table->schema = router->targetSchemaName;
        }

        if (SymStringUtils_isNotBlank(router->targetTableName)) {
            table->name = router->targetTableName;
        }
    }
    return table;
}

SymCsvData * SymExtractDataReader_nextData(SymExtractDataReader *this) {
    SymData *data = NULL;
    if (this->iter->hasNext(this->iter)) {
        data = (SymData *) this->iter->next(this->iter);
        SymTriggerHistory *triggerHistory = data->triggerHistory;

        if (strcmp(data->eventType, SYM_DATA_EVENT_RELOAD) == 0) {
            // TODO: implement outgoing reload event
        } else {
            SymTrigger *trigger = this->triggerRouterService->getTriggerById(this->triggerRouterService, triggerHistory->triggerId, 0);
            if (trigger) {
                if (this->lastTriggerHistory == NULL || this->lastTriggerHistory->triggerHistoryId != triggerHistory->triggerHistoryId ||
                        this->lastRouterId == NULL || strcmp(this->lastRouterId, data->routerId) != 0) {
                    this->sourceTable = lookupAndOrderColumnsAccordingToTriggerHistory(this, data->routerId, triggerHistory, 0, 1);
                    this->targetTable = lookupAndOrderColumnsAccordingToTriggerHistory(this, data->routerId, triggerHistory, 1, 0);
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
        SymDataService *dataService, SymTriggerRouterService *triggerRouterService, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymExtractDataReader *) calloc(1, sizeof(SymExtractDataReader));
    }
    this->dataService = dataService;
    this->triggerRouterService = triggerRouterService;
    this->platform = platform;
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
