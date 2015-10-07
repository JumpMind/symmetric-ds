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
#include "io/writer/DefaultDatabaseWriter.h"

void SymDefaultDatabaseWriter_open(SymDefaultDatabaseWriter *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    this->sqlTransaction = sqlTemplate->startSqlTransaction(sqlTemplate);
}

void SymDefaultDatabaseWriter_startBatch(SymDefaultDatabaseWriter *this, SymBatch *batch) {
    this->batch = batch;
    this->isError = 0;

    // IDataProcessorListener.beforeBatchStarted
    // TODO: if batchId < 0, remove outgoing configuration batches
    this->incomingBatch = SymIncomingBatch_new(NULL);
    this->incomingBatch->batchId = batch->batchId;
    SymDataWriter *super = (SymDataWriter *) &this->super;
    super->batchesProcessed->add(super->batchesProcessed, this->incomingBatch);

    // IDataProcessorListener.afterBatchStarted
    this->dialect->disableSyncTriggers(this->dialect, this->sqlTransaction, batch->sourceNodeId);
}

unsigned short SymDefaultDatabaseWriter_startTable(SymDefaultDatabaseWriter *this, SymTable *table) {
    this->dmlStatement = NULL;
    this->sourceTable = table;

    SymTable *targetTable = this->platform->getTableFromCache(this->platform, table->catalog, table->schema, table->name, 0);

    if (targetTable) {
        // TODO: cache the filtered table
        this->targetTable = targetTable->copyAndFilterColumns(targetTable, table, 1);
    } else {
        this->targetTable = table;
    }
    return 1;
}

unsigned short SymDefaultDatabaseWriter_requiresNewStatement(SymDefaultDatabaseWriter *this, SymDmlType currentDmlType, SymCsvData *data) {
    unsigned short requiresNew = this->dmlStatement == NULL || this->dmlStatement->dmlType != currentDmlType;
    return requiresNew;
}

void SymDefaultDatabaseWriter_insert(SymDefaultDatabaseWriter *this, SymCsvData *data) {
    if (SymDefaultDatabaseWriter_requiresNewStatement(this, SYM_DML_TYPE_INSERT, data)) {
        if (this->dmlStatement) {
            this->sqlTransaction->close(this->sqlTransaction);
            this->dmlStatement->destroy(this->dmlStatement);
        }
        // TODO: pass nullKeyIndiciators
        this->dmlStatement = SymDmlStatement_new(NULL, SYM_DML_TYPE_INSERT, this->targetTable, NULL, &this->platform->databaseInfo);
        this->sqlTransaction->prepare(this->sqlTransaction, this->dmlStatement->sql);
    }
    // TODO: need to know length of each rowData
    this->sqlTransaction->addRow(this->sqlTransaction, data->rowData, this->dmlStatement->sqlTypes);
}

void SymDefaultDatabaseWriter_update(SymDefaultDatabaseWriter *this, SymCsvData *data) {
    printf("update\n");
    if (SymDefaultDatabaseWriter_requiresNewStatement(this, SYM_DML_TYPE_UPDATE, data)) {
        if (this->dmlStatement) {
            this->sqlTransaction->close(this->sqlTransaction);
            this->dmlStatement->destroy(this->dmlStatement);
        }
        // TODO: pass nullKeyIndiciators
        this->dmlStatement = SymDmlStatement_new(NULL, SYM_DML_TYPE_UPDATE, this->targetTable, NULL, &this->platform->databaseInfo);
        this->sqlTransaction->prepare(this->sqlTransaction, this->dmlStatement->sql);
    }
    // TODO: need to know length of each rowData and pkData
    SymStringArray *values = SymStringArray_new(NULL);
    values->addAll(values, data->rowData);
    values->addAll(values, data->pkData);
    this->sqlTransaction->addRow(this->sqlTransaction, values, this->dmlStatement->sqlTypes);
    values->destroy(values);
}

void SymDefaultDatabaseWriter_delete(SymDefaultDatabaseWriter *this, SymCsvData *data) {
    printf("delete\n");
    if (SymDefaultDatabaseWriter_requiresNewStatement(this, SYM_DML_TYPE_DELETE, data)) {
        if (this->dmlStatement) {
            this->sqlTransaction->close(this->sqlTransaction);
            this->dmlStatement->destroy(this->dmlStatement);
        }
        // TODO: pass nullKeyIndiciators
        this->dmlStatement = SymDmlStatement_new(NULL, SYM_DML_TYPE_UPDATE, this->targetTable, NULL, &this->platform->databaseInfo);
        this->sqlTransaction->prepare(this->sqlTransaction, this->dmlStatement->sql);
    }
    // TODO: need to know length of each pkData
    this->sqlTransaction->addRow(this->sqlTransaction, data->pkData, this->dmlStatement->sqlTypes);
}

void SymDefaultDatabaseWriter_sql(SymDefaultDatabaseWriter *this, SymCsvData *data) {
    if (this->dmlStatement) {
        this->sqlTransaction->close(this->sqlTransaction);
        this->dmlStatement->destroy(this->dmlStatement);
    }
    int error;
    this->sqlTransaction->update(this->sqlTransaction, data->rowData->array[0], NULL, NULL, &error);
}

unsigned short SymDefaultDatabaseWriter_write(SymDefaultDatabaseWriter *this, SymCsvData *data) {
    switch (data->dataEventType) {
    case SYM_DATA_EVENT_INSERT:
        SymDefaultDatabaseWriter_insert(this, data);
        break;
    case SYM_DATA_EVENT_UPDATE:
        SymDefaultDatabaseWriter_update(this, data);
        break;
    case SYM_DATA_EVENT_DELETE:
        SymDefaultDatabaseWriter_delete(this, data);
        break;
    case SYM_DATA_EVENT_SQL:
        SymDefaultDatabaseWriter_sql(this, data);
        break;
    }
    return 1;
}

void SymDefaultDatabaseWriter_endTable(SymDefaultDatabaseWriter *this, SymTable *table) {
}

void SymDefaultDatabaseWriter_endBatch(SymDefaultDatabaseWriter *this, SymBatch *batch) {
    // IDataProcessorListener.beforeBatchEnd
    this->dialect->enableSyncTriggers(this->dialect, this->sqlTransaction);

    this->dmlStatement = NULL;
    if (batch->isIgnore) {
        this->incomingBatch->ignoreCount++;
    }

    if (!this->isError) {
        this->sqlTransaction->commit(this->sqlTransaction);

        // IDataProcessorListener.batchSuccessful
        // TODO: update batch statistics
        this->incomingBatch->status = SYM_INCOMING_BATCH_STATUS_OK;
        if (this->incomingBatchService->isRecordOkBatchesEnabled(this->incomingBatchService)) {
            this->incomingBatchService->updateIncomingBatch(this->incomingBatchService, this->incomingBatch);
        } else if (this->incomingBatch->retry) {
            this->incomingBatchService->deleteIncomingBatch(this->incomingBatchService, this->incomingBatch);
        }
    } else {
        this->sqlTransaction->rollback(this->sqlTransaction);

        // IDataProcessorListener.batchInError
        // TODO: update batch statistics
        // TODO: update batch sql code, state, message

        if (this->incomingBatchService->isRecordOkBatchesEnabled(this->incomingBatchService) || this->incomingBatch->retry) {
            this->incomingBatchService->updateIncomingBatch(this->incomingBatchService, this->incomingBatch);
        } else {
            this->incomingBatchService->insertIncomingBatch(this->incomingBatchService, this->incomingBatch);
        }
    }
}

void SymDefaultDatabaseWriter_close(SymDefaultDatabaseWriter *this) {
    printf("close\n");
    this->sqlTransaction->close(this->sqlTransaction);
}

void SymDefaultDatabaseWriter_destroy(SymDefaultDatabaseWriter *this) {
    free(this);
}

SymDefaultDatabaseWriter * SymDefaultDatabaseWriter_new(SymDefaultDatabaseWriter *this, SymIncomingBatchService *incomingBatchService,
        SymDatabasePlatform *platform, SymDialect *dialect) {
    if (this == NULL) {
        this = (SymDefaultDatabaseWriter *) calloc(1, sizeof(SymDefaultDatabaseWriter));
    }
    SymDataWriter *super = &this->super;
    this->incomingBatchService = incomingBatchService;
    this->platform = platform;
    this->dialect = dialect;
    super->batchesProcessed = SymList_new(NULL);
    super->open = (void *) &SymDefaultDatabaseWriter_open;
    super->startBatch = (void *) &SymDefaultDatabaseWriter_startBatch;
    super->startTable = (void *) &SymDefaultDatabaseWriter_startTable;
    super->write = (void *) &SymDefaultDatabaseWriter_write;
    super->endTable = (void *) &SymDefaultDatabaseWriter_endTable;
    super->endBatch = (void *) &SymDefaultDatabaseWriter_endBatch;
    super->close = (void *) &SymDefaultDatabaseWriter_close;
    super->destroy = (void *) &SymDefaultDatabaseWriter_destroy;
    return this;
}
