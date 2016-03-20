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
#include "io/writer/ProtocolDataWriter.h"

void SymProtocolDataWriter_open(SymProtocolDataWriter *this) {
    this->sb = SymStringBuilder_newWithSize(4096);
    this->reader->open(this->reader);
}

static void SymProtocolDataWriter_println(SymStringBuilder *sb, char *token, char *value) {
    sb->append(sb, token)->append(sb, ",")->append(sb, value)->append(sb, "\n");
}

static void SymProtocolDataWriter_println2(SymStringBuilder *sb, char *token, char *value1, char *value2) {
    sb->append(sb, token)->append(sb, ",")->append(sb, value1);
    sb->append(sb, ",")->append(sb, value2)->append(sb, "\n");
}

static void SymProtocolDataWriter_printlnl(SymStringBuilder *sb, char *token, long value) {
    sb->append(sb, token)->append(sb, ",")->appendf(sb, "%ld", value)->append(sb, "\n");
}

static void SymProtocolDataWriter_printList(SymStringBuilder *buffer, char *token, SymList *list) {
    buffer->append(buffer, token)->append(buffer, ",");
    SymIterator *iter = list->iterator(list);
    while (iter->hasNext(iter)) {
        SymColumn *column = (SymColumn *) iter->next(iter);
        if (iter->index > 0) {
            buffer->append(buffer, ",");
        }
        buffer->append(buffer, column->name);
    }
    buffer->append(buffer, "\n");
    iter->destroy(iter);
}

static void SymProtocolDataWriter_startBatch(SymProtocolDataWriter *this, SymBatch *batch) {
    if (this->isFirstBatch) {
        this->isFirstBatch = 0;
        SymProtocolDataWriter_println(this->sb, SYM_CSV_NODEID, this->sourceNodeId);
        SymProtocolDataWriter_println(this->sb, SYM_CSV_BINARY, SYM_BINARY_ENCODING_HEX);
    }
    if (!SymStringUtils_isBlank(batch->channelId)) {
        SymProtocolDataWriter_println(this->sb, SYM_CSV_CHANNEL, batch->channelId);
    }
    SymProtocolDataWriter_printlnl(this->sb, SYM_CSV_BATCH, batch->batchId);
    this->batch = batch;
}

static void SymProtocolDataWriter_endBatch(SymProtocolDataWriter *this, SymBatch *batch) {
    if (batch->isIgnore) {
        SymProtocolDataWriter_println(this->sb, SYM_CSV_IGNORE, NULL);
    }
    SymProtocolDataWriter_printlnl(this->sb, SYM_CSV_COMMIT, batch->batchId);

    this->batch = NULL;
}

static void SymProtocolDataWriter_startTable(SymProtocolDataWriter *this, SymTable *table, SymBatch *batch) {
    if (!batch->isIgnore) {
        this->table = table;

        SymProtocolDataWriter_println(this->sb, SYM_CSV_CATALOG, table->catalog);
        SymProtocolDataWriter_println(this->sb, SYM_CSV_SCHEMA, table->schema);

        char *tableKey = table->getTableKey(table);
        char *fullyQualifiedTableName = table->getFullyQualifiedTableName(table);
        char *previousTableKey = this->processedTables->get(this->processedTables, fullyQualifiedTableName);
        SymProtocolDataWriter_println(this->sb, SYM_CSV_TABLE, table->name);
        if (!SymStringUtils_equals(tableKey, previousTableKey)) {
            SymList *pkList = table->getPrimaryKeyColumns(table);
            SymProtocolDataWriter_printList(this->sb, SYM_CSV_KEYS, pkList);
            SymProtocolDataWriter_printList(this->sb, SYM_CSV_COLUMNS, table->columns);
            this->processedTables->put(this->processedTables, fullyQualifiedTableName, tableKey);
            pkList->destroy(pkList);
        }
        free(fullyQualifiedTableName);
        free(tableKey);
    }
}

static void SymProtocolDataWriter_endTable(SymProtocolDataWriter *this, SymTable *table, SymBatch *batch) {
    this->table = NULL;
}

static void SymProtocolDataWriter_writeData(SymProtocolDataWriter *this, SymTable *table, SymBatch *batch, SymData *data) {
    if (!batch->isIgnore) {
        if (data->eventType == SYM_DATA_EVENT_INSERT) {
            SymProtocolDataWriter_println(this->sb, SYM_CSV_INSERT, data->rowData);
        } else if (data->eventType == SYM_DATA_EVENT_UPDATE) {
            if (data->oldData) {
                SymProtocolDataWriter_println(this->sb, SYM_CSV_OLD, data->oldData);
            }
            SymProtocolDataWriter_println2(this->sb, SYM_CSV_UPDATE, data->rowData, data->pkData);
        } else if (data->eventType == SYM_DATA_EVENT_DELETE) {
            if (data->oldData) {
                SymProtocolDataWriter_println(this->sb, SYM_CSV_OLD, data->oldData);
            }
            SymProtocolDataWriter_println(this->sb, SYM_CSV_DELETE, data->pkData);
        }
    }
}

size_t SymProtocolDataWriter_process(SymProtocolDataWriter *this, char *buffer, size_t size, size_t count) {
    SymBatch *batch = NULL;
    SymTable *table = NULL;
    SymData *data = NULL;
    unsigned short bufferFull = 0;

    while (!bufferFull && (this->batch || (batch = this->reader->nextBatch(this->reader)))) {
        if (!this->batch) {
            SymProtocolDataWriter_startBatch(this, batch);
        } else {
           batch = this->batch;
        }

        while (!bufferFull && (this->table || (table = this->reader->nextTable(this->reader)))) {
            if (!this->table) {
                SymProtocolDataWriter_startTable(this, table, batch);
            }

            while (!bufferFull && (data = this->reader->nextData(this->reader))) {
                SymProtocolDataWriter_writeData(this, table, batch, data);
                if (this->sb->pos >= count) {
                    bufferFull = 1;
                }
            }
            if (!bufferFull) {
                SymProtocolDataWriter_endTable(this, table, batch);
            }
        }
        if (!bufferFull) {
            SymProtocolDataWriter_endBatch(this, batch);
        }
    }

    size_t numBytes;
    if (this->sb->pos < count) {
        numBytes = this->sb->pos * size;
    } else {
        numBytes = count * size;
        bufferFull = 1;
    }

    if (numBytes > 0) {
        memcpy(buffer, this->sb->str, numBytes);
        SymLog_debug("Writing data: %.*s\n", numBytes, this->sb->str);
    }

    if (bufferFull) {
        char *leftOverData = this->sb->substring(this->sb, count, this->sb->pos);
        this->sb->reset(this->sb);
        this->sb->append(this->sb, leftOverData);
        free(leftOverData);
    } else {
        this->sb->reset(this->sb);
    }
    return numBytes;
}

SymList * SymProtocolDataWriter_getBatchesProcessed(SymProtocolDataWriter *this) {
    return this->reader->batchesProcessed;
}

void SymProtocolDataWriter_close(SymProtocolDataWriter *this) {
    this->reader->close(this->reader);
    this->sb->destroy(this->sb);
}

void SymProtocolDataWriter_destroy(SymProtocolDataWriter *this) {
    this->processedTables->destroy(this->processedTables);
    free(this);
}

SymProtocolDataWriter * SymProtocolDataWriter_new(SymProtocolDataWriter *this, char *sourceNodeId, SymDataReader *reader) {
    if (this == NULL) {
        this = (SymProtocolDataWriter *) calloc(1, sizeof(SymProtocolDataWriter));
    }
    this->sourceNodeId = sourceNodeId;
    this->reader = reader;
    this->isFirstBatch = 1;
    this->processedTables = SymMap_new(NULL, 100);

    SymDataProcessor *super = &this->super;
    super->open = (void *) &SymProtocolDataWriter_open;
    super->close = (void *) &SymProtocolDataWriter_close;
    super->process = (void *) &SymProtocolDataWriter_process;
    super->getBatchesProcessed = (void *) &SymProtocolDataWriter_getBatchesProcessed;
    super->destroy = (void *) &SymProtocolDataWriter_destroy;
    return this;
}
