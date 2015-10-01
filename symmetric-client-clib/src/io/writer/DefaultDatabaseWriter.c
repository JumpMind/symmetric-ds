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
    printf("open\n");
    SymSqlTemplate *sqlTemplate = this->platform->get_sql_template(this->platform);
    this->sqlTransaction = sqlTemplate->start_sql_transaction(sqlTemplate);
}

void SymDefaultDatabaseWriter_start_batch(SymDefaultDatabaseWriter *this, SymBatch *batch) {
    printf("start batch %ld\n", batch->batchId);
    this->batch = batch;
    this->isError = 0;
}

unsigned short SymDefaultDatabaseWriter_start_table(SymDefaultDatabaseWriter *this, SymTable *table) {
    printf("start table %s\n", table->name);
    this->dmlStatement = NULL;
    this->sourceTable = table;

    SymTable *targetTable = this->platform->get_table_from_cache(this->platform, table->catalog, table->schema, table->name, 0);

    if (targetTable) {
        // TODO: cache the filtered table
        this->targetTable = targetTable->copy_and_filter_columns(targetTable, table, 1);
    } else {
        this->targetTable = table;
    }
    return 1;
}

unsigned short SymDefaultDatabaseWriter_requires_new_statement(SymDefaultDatabaseWriter *this, SymDmlType currentDmlType, SymCsvData *data) {
    unsigned short requiresNew = this->dmlStatement == NULL || this->dmlStatement->dmlType != currentDmlType;
    return requiresNew;
}

void SymDefaultDatabaseWriter_insert(SymDefaultDatabaseWriter *this, SymCsvData *data) {
    printf("insert");
    if (SymDefaultDatabaseWriter_requires_new_statement(this, SYM_DML_TYPE_INSERT, data)) {
        if (this->dmlStatement) {
            this->sqlTransaction->close(this->sqlTransaction);
            this->dmlStatement->destroy(this->dmlStatement);
        }
        this->dmlStatement = SymDmlStatement_new(NULL, SYM_DML_TYPE_INSERT, this->targetTable, NULL, &this->platform->databaseInfo);
        this->sqlTransaction->prepare(this->sqlTransaction, this->dmlStatement->sql);
    }
    // TODO: need to know length of each rowData
    this->sqlTransaction->add_row(this->sqlTransaction, data->rowData, this->dmlStatement->sqlTypes);
}

void SymDefaultDatabaseWriter_update(SymDefaultDatabaseWriter *this, SymCsvData *data) {

}

void SymDefaultDatabaseWriter_delete(SymDefaultDatabaseWriter *this, SymCsvData *data) {

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

void SymDefaultDatabaseWriter_end_table(SymDefaultDatabaseWriter *this, SymTable *table) {
    printf("end table %s\n", table->name);
}

void SymDefaultDatabaseWriter_end_batch(SymDefaultDatabaseWriter *this, SymBatch *batch) {
    printf("end batch %ld\n", batch->batchId);
    this->dmlStatement = NULL;
    if (!this->isError) {
        this->sqlTransaction->commit(this->sqlTransaction);
    } else {
        this->sqlTransaction->rollback(this->sqlTransaction);
    }
}

void SymDefaultDatabaseWriter_close(SymDefaultDatabaseWriter *this) {
    printf("close\n");
    this->sqlTransaction->close(this->sqlTransaction);
}

void SymDefaultDatabaseWriter_destroy(SymDefaultDatabaseWriter *this) {
    free(this);
}

SymDefaultDatabaseWriter * SymDefaultDatabaseWriter_new(SymDefaultDatabaseWriter *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymDefaultDatabaseWriter *) calloc(1, sizeof(SymDefaultDatabaseWriter));
    }
    SymDataWriter *super = &this->super;
    this->platform = platform;
    super->open = (void *) &SymDefaultDatabaseWriter_open;
    super->start_batch = (void *) &SymDefaultDatabaseWriter_start_batch;
    super->start_table = (void *) &SymDefaultDatabaseWriter_start_table;
    super->write = (void *) &SymDefaultDatabaseWriter_write;
    super->end_table = (void *) &SymDefaultDatabaseWriter_end_table;
    super->end_batch = (void *) &SymDefaultDatabaseWriter_end_batch;
    super->close = (void *) &SymDefaultDatabaseWriter_close;
    super->destroy = (void *) &SymDefaultDatabaseWriter_destroy;
    return this;
}
