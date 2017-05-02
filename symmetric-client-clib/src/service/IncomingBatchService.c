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
#include "service/IncomingBatchService.h"
#include "common/Log.h"

static SymIncomingBatch * SymIncomingBatchService_mapIncomingBatch(SymRow *row) {
    SymIncomingBatch *batch = SymIncomingBatch_new(NULL);
    batch->batchId = row->getLong(row, "batch_id");
    batch->nodeId = row->getStringNew(row, "node_id");
    batch->channelId = row->getStringNew(row, "channel_id");
    batch->status = row->getStringNew(row, "status");
    batch->networkMillis = row->getInt(row, "network_millis");
    batch->filterMillis = row->getLong(row, "filter_millis");
    batch->databaseMillis = row->getLong(row, "database_millis");
    batch->failedRowNumber = row->getLong(row, "failed_row_number");
    batch->failedLineNumber = row->getLong(row, "failed_line_number");
    batch->byteCount = row->getLong(row, "byte_count");
    batch->statementCount = row->getLong(row, "statement_count");
    batch->fallbackInsertCount = row->getLong(row, "fallback_insert_count");
    batch->fallbackUpdateCount = row->getLong(row, "fallback_update_count");
    batch->ignoreCount = row->getLong(row, "ignore_count");
    batch->missingDeleteCount = row->getLong(row, "missing_delete_count");
    batch->skipCount = row->getLong(row, "skip_count");
    batch->sqlState = row->getStringNew(row, "sql_state");
    batch->sqlCode = row->getInt(row, "sql_code");
    batch->sqlMessage = row->getStringNew(row, "sql_message");
    batch->lastUpdatedHostName = row->getStringNew(row, "last_update_hostname");
    batch->lastUpdatedTime = row->getDate(row, "last_update_time");
    batch->createTime = row->getDate(row, "create_time");
    batch->errorFlag = row->getInt(row, "error_flag");
    return batch;
}

SymIncomingBatch * SymIncomingBatchService_findIncomingBatch(SymIncomingBatchService *this, long batchId, char *nodeId) {
    SymStringBuilder *sql = SymStringBuilder_newWithString(SYM_SQL_SELECT_INCOMING_BATCH_PREFIX);
    sql->append(sql, SYM_SQL_FIND_INCOMING_BATCH);
    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, batchId)->add(args, nodeId);

    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    SymList *batches = sqlTemplate->query(sqlTemplate, sql->str, args, NULL, &error, (void *) SymIncomingBatchService_mapIncomingBatch);
    SymIncomingBatch *incomingBatch = (SymIncomingBatch *) batches->get(batches, 0);
    sql->destroy(sql);
    args->destroy(args);
    batches->destroy(batches);
    return incomingBatch;
}

unsigned short SymIncomingBatchService_isRecordOkBatchesEnabled(SymIncomingBatchService *this) {
    return this->parameterService->is(this->parameterService, SYM_PARAMETER_INCOMING_BATCH_RECORD_OK_ENABLED, 1);
}

unsigned short SymIncomingBatchService_acquireIncomingBatch(SymIncomingBatchService *this, SymIncomingBatch *batch) {
    unsigned short okayToProcess = 1;
    if (batch->batchId > 0) {
        SymIncomingBatch *existingBatch = NULL;

        if (this->isRecordOkBatchesEnabled(this)) {
            existingBatch = this->findIncomingBatch(this, batch->batchId, batch->nodeId);
            if (existingBatch == NULL) {
                this->insertIncomingBatch(this, batch);
            } else {
                batch->retry = 1;
            }
        } else {
            existingBatch = this->findIncomingBatch(this, batch->batchId, batch->nodeId);
            if (existingBatch != NULL) {
                batch->retry = 1;
            }
        }

        if (batch->retry) {
            if (strcmp(existingBatch->status, SYM_INCOMING_BATCH_STATUS_ERROR) == 0
                    || strcmp(existingBatch->status, SYM_INCOMING_BATCH_STATUS_LOADING) == 0
                    || !this->parameterService->is(this->parameterService, SYM_PARAMETER_INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED, 1)) {
                okayToProcess = 1;
                existingBatch->status = SYM_INCOMING_BATCH_STATUS_LOADING;
                SymLog_info("Retrying batch %s-%ld", batch->nodeId, batch->batchId);
            } else if (strcmp(existingBatch->status, SYM_INCOMING_BATCH_STATUS_IGNORED) == 0) {
                okayToProcess = 0;
                batch->status = SYM_INCOMING_BATCH_STATUS_OK;
                batch->ignoreCount++;
                existingBatch->status = SYM_INCOMING_BATCH_STATUS_OK;
                existingBatch->ignoreCount++;
                SymLog_info("Ignoring batch %s-%ld", batch->nodeId, batch->batchId);
            } else {
                okayToProcess = 0;
                batch->status = existingBatch->status;
                batch->byteCount = existingBatch->byteCount;
                batch->databaseMillis = existingBatch->databaseMillis;
                batch->networkMillis = existingBatch->networkMillis;
                batch->filterMillis = existingBatch->filterMillis;
                batch->skipCount = existingBatch->skipCount + 1;
                batch->statementCount = existingBatch->statementCount;
                existingBatch->skipCount++;
                SymLog_info("Skipping batch %s-%ld", batch->nodeId, batch->batchId);
            }
            this->updateIncomingBatch(this, existingBatch);
            existingBatch->destroy(existingBatch);
        }
    }
    return okayToProcess;
}

int SymIncomingBatchService_insertIncomingBatch(SymIncomingBatchService *this, SymIncomingBatch *batch) {
    int count = 0;
    if (batch->batchId > 0) {
        batch->lastUpdatedHostName = SymAppUtils_getHostName();
        batch->lastUpdatedTime = SymDate_new();

        SymStringArray *args = SymStringArray_new(NULL);
        args->addLong(args, batch->batchId)->add(args, batch->nodeId)->add(args, batch->channelId);
        args->add(args, batch->status)->addLong(args, batch->networkMillis)->addLong(args, batch->filterMillis);
        args->addLong(args, batch->databaseMillis)->addLong(args, batch->failedRowNumber)->addLong(args, batch->failedLineNumber);
        args->addLong(args, batch->byteCount)->addLong(args, batch->statementCount)->addLong(args, batch->fallbackInsertCount);
        args->addLong(args, batch->fallbackUpdateCount)->addLong(args, batch->ignoreCount)->addLong(args, batch->missingDeleteCount);
        args->addLong(args, batch->skipCount)->add(args, batch->sqlState)->addInt(args, batch->sqlCode);
        args->add(args, batch->sqlMessage)->add(args, batch->lastUpdatedHostName)->add(args, batch->lastUpdatedTime->dateTimeString);

        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
        int error;
        count = sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_INCOMING_BATCH, args, NULL, &error);
        args->destroy(args);
    }
    return count;
}

int SymIncomingBatchService_updateIncomingBatch(SymIncomingBatchService *this, SymIncomingBatch *batch) {
    int count = 0;
    if (batch->batchId > 0) {
        if (strcmp(batch->status, SYM_INCOMING_BATCH_STATUS_ERROR) == 0) {
            batch->errorFlag = 1;
        } else if (strcmp(batch->status, SYM_INCOMING_BATCH_STATUS_OK) == 0) {
            batch->errorFlag = 0;
        }
        batch->lastUpdatedHostName = SymAppUtils_getHostName();
        batch->lastUpdatedTime = SymDate_new();

        SymStringArray *args = SymStringArray_new(NULL);
        args->add(args, batch->status)->addInt(args, batch->errorFlag);
        args->addLong(args, batch->networkMillis)->addLong(args, batch->filterMillis);
        args->addLong(args, batch->databaseMillis)->addLong(args, batch->failedRowNumber)->addLong(args, batch->failedLineNumber);
        args->addLong(args, batch->byteCount)->addLong(args, batch->statementCount)->addLong(args, batch->fallbackInsertCount);
        args->addLong(args, batch->fallbackUpdateCount)->addLong(args, batch->ignoreCount)->addLong(args, batch->missingDeleteCount);
        args->addLong(args, batch->skipCount)->add(args, batch->sqlState)->addInt(args, batch->sqlCode);
        args->add(args, batch->sqlMessage)->add(args, batch->lastUpdatedHostName)->add(args, batch->lastUpdatedTime->dateTimeString);
        args->addLong(args, batch->batchId)->add(args, batch->nodeId);

        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
        int error;
        count = sqlTemplate->update(sqlTemplate, SYM_SQL_UPDATE_INCOMING_BATCH, args, NULL, &error);
        args->destroy(args);
    }
    return count;
}

int SymIncomingBatchService_deleteIncomingBatch(SymIncomingBatchService *this, SymIncomingBatch *batch) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, batch->batchId)->add(args, batch->nodeId);

    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    int count = sqlTemplate->update(sqlTemplate, SYM_SQL_DELETE_INCOMING_BATCH, args, NULL, &error);
    args->destroy(args);
    return count;
}

int SymIncomingBatchService_countIncomingBatchesInError(SymIncomingBatchService *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    return sqlTemplate->queryForInt(sqlTemplate, SYM_SQL_COUNT_INCOMING_BATCHES_ERRORS, NULL, NULL, &error);
}

void SymIncomingBatchService_destroy(SymIncomingBatchService *this) {
    free(this);
}

SymIncomingBatchService * SymIncomingBatchService_new(SymIncomingBatchService *this, SymDatabasePlatform *platform, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymIncomingBatchService *) calloc(1, sizeof(SymIncomingBatchService));
    }
    this->platform = platform;
    this->parameterService = parameterService;
    this->findIncomingBatch = (void *) &SymIncomingBatchService_findIncomingBatch;
    this->acquireIncomingBatch = (void *) &SymIncomingBatchService_acquireIncomingBatch;
    this->insertIncomingBatch = (void *) &SymIncomingBatchService_insertIncomingBatch;
    this->updateIncomingBatch = (void *) &SymIncomingBatchService_updateIncomingBatch;
    this->deleteIncomingBatch = (void *) &SymIncomingBatchService_deleteIncomingBatch;
    this->isRecordOkBatchesEnabled = (void *) &SymIncomingBatchService_isRecordOkBatchesEnabled;
    this->countIncomingBatchesInError = (void *) &SymIncomingBatchService_countIncomingBatchesInError;
    this->destroy = (void *) &SymIncomingBatchService_destroy;
    return this;
}
