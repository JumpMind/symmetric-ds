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
#include "service/OutgoingBatchService.h"
#include "common/Log.h"

SymOutgoingBatch * SymOutgoingBatchService_outgoingBatchMapper(SymRow *row) {
    SymOutgoingBatch *batch = SymOutgoingBatch_new(NULL);
    batch->nodeId = row->getStringNew(row, "node_id");
    batch->status = row->getStringNew(row, "status");
    batch->batchId = row->getLong(row, "batch_id");
    batch->channelId = row->getStringNew(row, "channel_id");
    batch->byteCount = row->getLong(row, "byte_count");
    batch->extractCount = row->getLong(row, "extract_count");
    batch->sentCount = row->getLong(row, "sent_count");
    batch->loadCount = row->getLong(row, "load_count");
    batch->dataEventCount = row->getLong(row, "data_event_count");
    batch->reloadEventCount = row->getLong(row, "reload_event_count");
    batch->insertEventCount = row->getLong(row, "insert_event_count");
    batch->updateEventCount = row->getLong(row, "update_event_count");
    batch->deleteEventCount = row->getLong(row, "delete_event_count");
    batch->otherEventCount = row->getLong(row, "other_event_count");
    batch->ignoreCount = row->getLong(row, "ignore_count");
    batch->routerMillis = row->getLong(row, "router_millis");
    batch->networkMillis = row->getLong(row, "network_millis");
    batch->filterMillis = row->getLong(row, "filter_millis");
    batch->loadMillis = row->getLong(row, "load_millis");
    batch->extractMillis = row->getLong(row, "extract_millis");
    batch->sqlState = row->getStringNew(row, "sql_state");
    batch->sqlCode = row->getInt(row, "sql_code");
    batch->sqlMessage = row->getStringNew(row, "sql_message");
    batch->failedDataId = row->getLong(row, "failed_data_id");
    batch->lastUpdatedHostName = row->getStringNew(row, "last_update_hostname");
    batch->lastUpdatedTime = row->getDate(row, "last_update_time");
    batch->createTime = row->getDate(row, "create_time");
    batch->loadFlag = row->getBoolean(row, "load_flag");
    batch->errorFlag = row->getBoolean(row, "error_flag");
    batch->commonFlag = row->getBoolean(row, "common_flag");
    batch->extractJobFlag = row->getBoolean(row, "extract_job_flag");
    batch->loadId = row->getLong(row, "load_id");
    batch->createBy = row->getStringNew(row, "create_by");
    return batch;
}

void SymOutgoingBatchService_insertOutgoingBatch(SymOutgoingBatchService *this, SymOutgoingBatch *batch) {
    if (batch->batchId <= 0) {
        batch->batchId = this->sequenceService->nextVal(this->sequenceService, SYM_SEQUENCE_OUTGOING_BATCH);
    }

    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, batch->batchId)->add(args, batch->nodeId)->add(args, batch->channelId);
    args->add(args, batch->status)->addInt(args, batch->loadId)->addInt(args, batch->extractJobFlag);
    args->addInt(args, batch->loadFlag)->addInt(args, batch->commonFlag)->addLong(args, batch->reloadEventCount);
    args->addLong(args, batch->otherEventCount)->add(args, batch->lastUpdatedHostName)->add(args, batch->createBy);

    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_OUTGOING_BATCH, args, NULL, &error);

    args->destroy(args);
}

SymOutgoingBatch * SymOutgoingBatchService_findOutgoingBatch(SymOutgoingBatchService *this, long batchId, char *nodeId) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, batchId);
    args->add(args, nodeId);


    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_OUTGOING_BATCH_PREFIX);
    if (SymStringUtils_isNotBlank(nodeId)) {
        sb->append(sb, SYM_SQL_FIND_OUTGOING_BATCH);
    } else {
        sb->append(sb, SYM_SQL_FIND_OUTGOING_BATCH_BY_ID_ONLY);
    }

    int error;
    SymList *batches = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymOutgoingBatchService_outgoingBatchMapper);

    SymOutgoingBatch *batch = (SymOutgoingBatch *) batches->get(batches, 0);
    sb->destroy(sb);
    args->destroy(args);
    batches->destroy(batches);
    return batch;
}

SymOutgoingBatches * SymOutgoingBatchService_getOutgoingBatches(SymOutgoingBatchService *this, char *nodeId) {
    time_t ts = time(NULL);

    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_SELECT_OUTGOING_BATCH_PREFIX);
    sb->append(sb, SYM_SQL_SELECT_OUTGOING_BATCH);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, nodeId)->add(args, SYM_OUTGOING_BATCH_REQUEST)->add(args, SYM_OUTGOING_BATCH_NEW);
    args->add(args, SYM_OUTGOING_BATCH_QUERYING)->add(args, SYM_OUTGOING_BATCH_SENDING)->add(args, SYM_OUTGOING_BATCH_LOADING);
    args->add(args, SYM_OUTGOING_BATCH_ERROR)->add(args, SYM_OUTGOING_BATCH_IGNORED);

    // TODO: sqlTemplate->queryWithLimit with limit on results
    //int maxNumberOfBatchesToSelect = this->parameterService->getInt(this->parameterService, SYM_OUTGOING_BATCH_MAX_BATCHES_TO_SELECT, 1000);
    int error;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymList *list = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymOutgoingBatchService_outgoingBatchMapper);

    SymOutgoingBatches *batches = SymOutgoingBatches_newWithList(NULL, list);

    // TODO: sort channels by processing order and errors, and filter batches for only active channels

    // TODO: filter batches with channel window

    time_t executeTimeInMs = (time(NULL) - ts) * 1000;
    if (executeTimeInMs > SYM_LONG_OPERATION_THRESHOLD) {
        SymLog_info("Selecting %d outgoing batch rows for node %s took %ld ms", list->size, nodeId, executeTimeInMs);
    }

    sb->destroy(sb);
    args->destroy(args);
    list->destroy(list);
    return batches;
}

void SymOutgoingBatchService_updateOutgoingBatch(SymOutgoingBatchService *this, SymOutgoingBatch *batch) {
    batch->lastUpdatedTime = SymDate_new();
    batch->lastUpdatedHostName = SymAppUtils_getHostName();

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, batch->status)->addInt(args, batch->loadId)->addInt(args, batch->extractJobFlag);
    args->addInt(args, batch->loadFlag)->addInt(args, batch->errorFlag)->addLong(args, batch->byteCount);
    args->addLong(args, batch->extractCount)->addLong(args, batch->sentCount)->addLong(args, batch->loadCount);
    args->addLong(args, batch->dataEventCount)->addLong(args, batch->reloadEventCount)->addLong(args, batch->insertEventCount);
    args->addLong(args, batch->updateEventCount)->addLong(args, batch->deleteEventCount)->addLong(args, batch->otherEventCount);
    args->addLong(args, batch->ignoreCount)->addLong(args, batch->routerMillis)->addLong(args, batch->networkMillis);
    args->addLong(args, batch->filterMillis)->addLong(args, batch->loadMillis)->addLong(args, batch->extractMillis);
    args->add(args, batch->sqlState)->addInt(args, batch->sqlCode)->add(args, batch->sqlMessage);
    args->addLong(args, batch->failedDataId)->add(args, batch->lastUpdatedHostName)->add(args, batch->lastUpdatedTime->dateTimeString);
    args->addLong(args, batch->batchId)->add(args, batch->nodeId);

    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    sqlTemplate->update(sqlTemplate, SYM_SQL_UPDATE_OUTGOING_BATCH, args, NULL, &error);

    args->destroy(args);
}

int SymOutgoingBatchService_countOutgoingBatchesUnsent(SymOutgoingBatchService *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    return sqlTemplate->queryForInt(sqlTemplate, SYM_SQL_COUNT_OUTGOING_BATCHES_UNSENT, NULL, NULL, &error);
}

int SymOutgoingBatchService_countOutgoingBatchesInError(SymOutgoingBatchService *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    return sqlTemplate->queryForInt(sqlTemplate, SYM_SQL_COUNT_OUTGOING_BATCHES_ERRORS, NULL, NULL, &error);
}

void SymOutgoingBatchService_destroy(SymOutgoingBatchService *this) {
    free(this);
}

SymOutgoingBatchService * SymOutgoingBatchService_new(SymOutgoingBatchService *this, SymDatabasePlatform *platform, SymParameterService *parameterService,
        SymSequenceService *sequenceService) {
    if (this == NULL) {
        this = (SymOutgoingBatchService *) calloc(1, sizeof(SymOutgoingBatchService));
    }
    this->platform = platform;
    this->parameterService = parameterService;
    this->sequenceService = sequenceService;
    this->findOutgoingBatch = (void *) &SymOutgoingBatchService_findOutgoingBatch;
    this->getOutgoingBatches = (void *) &SymOutgoingBatchService_getOutgoingBatches;
    this->insertOutgoingBatch = (void *) &SymOutgoingBatchService_insertOutgoingBatch;
    this->updateOutgoingBatch = (void *) &SymOutgoingBatchService_updateOutgoingBatch;
    this->countOutgoingBatchesUnsent = (void *) &SymOutgoingBatchService_countOutgoingBatchesUnsent;
    this->countOutgoingBatchesInError = (void *) &SymOutgoingBatchService_countOutgoingBatchesInError;
    this->destroy = (void *) &SymOutgoingBatchService_destroy;
    return this;
}
