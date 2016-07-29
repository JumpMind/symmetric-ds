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
#include "io/reader/ProtocolDataReader.h"

static void SymProtocolDataReader_parseField(void *data, size_t size, void *userData) {
    SymProtocolDataReader *this = (SymProtocolDataReader *) userData;
    if (!this->isError) {
        this->fields->addn(this->fields, data, size);
    }
}

static void SymProtocolDataReader_parseLine(int eol, void *userData) {
    SymProtocolDataReader *this = (SymProtocolDataReader *) userData;
    SymBatch *batch = this->batch;
    SymStringArray *fields = this->fields;

    if (!this->isError && fields->size > 0) {
        char *token = fields->get(fields, 0);
        if (strcmp(token, SYM_CSV_INSERT) == 0) {
            SymCsvData *csvData = SymCsvData_new(NULL);
            csvData->rowData = fields->subarray(fields, 1, fields->size);
            csvData->dataEventType = SYM_DATA_EVENT_INSERT;
            this->isError = !this->writer->write(this->writer, csvData);
            csvData->destroy(csvData);
        } else if (strcmp(token, SYM_CSV_OLD) == 0) {
            if (this->oldData != NULL) {
                this->oldData->destroy(this->oldData);
            }
            this->oldData = fields->subarray(fields, 1, fields->size);
        } else if (strcmp(token, SYM_CSV_UPDATE) == 0) {
            SymCsvData *csvData = SymCsvData_new(NULL);
            csvData->rowData = fields->subarray(fields, 1, fields->size);
            csvData->pkData = fields->subarray(fields, 1, this->keys->size + 1);
            csvData->oldData = this->oldData;
            csvData->dataEventType = SYM_DATA_EVENT_UPDATE;
            this->isError = !this->writer->write(this->writer, csvData);
            csvData->destroy(csvData);
            this->oldData = NULL;
        } else if (strcmp(token, SYM_CSV_DELETE) == 0) {
            SymCsvData *csvData = SymCsvData_new(NULL);
            csvData->pkData = fields->subarray(fields, 1, fields->size);
            csvData->dataEventType = SYM_DATA_EVENT_DELETE;
            this->isError = !this->writer->write(this->writer, csvData);
            csvData->destroy(csvData);
        } else if (strcmp(token, SYM_CSV_CATALOG) == 0) {
            SymStringBuilder_copyToField(&this->catalog, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_SCHEMA) == 0) {
            SymStringBuilder_copyToField(&this->schema, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_TABLE) == 0) {
            char *tableName = fields->get(fields, 1);
            this->table = (SymTable *) this->parsedTables->get(this->parsedTables, tableName);
            if (this->table) {
                this->writer->startTable(this->writer, this->table);
            } else {
                this->table = SymTable_newWithFullname(NULL, this->catalog, this->schema, tableName);
            }
        } else if (strcmp(token, SYM_CSV_KEYS) == 0) {
            this->keys = fields->subarray(fields, 1, fields->size);
        } else if (strcmp(token, SYM_CSV_COLUMNS) == 0) {
            this->table->columns = SymList_new(NULL);
            int i, isPrimary;
            for (i = 1; i < this->fields->size; i++) {
                isPrimary = this->keys->contains(this->keys, this->fields->array[i]);
                this->table->columns->add(this->table->columns, SymColumn_new(NULL, this->fields->array[i], isPrimary));
            }
            this->parsedTables->put(this->parsedTables, this->table->name, this->table);
            this->writer->startTable(this->writer, this->table);
        } else if (strcmp(token, SYM_CSV_NODEID) == 0) {
            SymStringBuilder_copyToField(&batch->sourceNodeId, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_CHANNEL) == 0) {
            SymStringBuilder_copyToField(&batch->channelId, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_BATCH) == 0) {
            batch->batchId = atol(fields->get(fields, 1));
            this->writer->startBatch(this->writer, batch);
        } else if (strcmp(token, SYM_CSV_BINARY) == 0) {
            this->batch->binaryEncoding = SymBinaryEncoding_valueOf(fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_COMMIT) == 0) {
            this->writer->endBatch(this->writer, batch);
        } else if (strcmp(token, SYM_CSV_IGNORE) == 0) {
            batch->isIgnore = 1;
        } else if (strcmp(token, SYM_CSV_SQL) == 0) {
            SymCsvData *csvData = SymCsvData_new(NULL);
            csvData->rowData = fields->subarray(fields, 1, fields->size);
            csvData->dataEventType = SYM_DATA_EVENT_SQL;
            this->isError = !this->writer->write(this->writer, csvData);
            csvData->destroy(csvData);
        }
        if (this->isError) {
            this->writer->endBatch(this->writer, batch);
        }
        fields->reset(fields);
    }
}

void SymProtocolDataReader_open(SymProtocolDataReader *this) {
    csv_init(this->csvParser, 0);
    this->writer->open(this->writer);
}

size_t SymProtocolDataReader_process(SymProtocolDataReader *this, char *data, size_t size, size_t count) {
    size_t length = size * count;

    char * dataWithReplacedQuotes = SymStringUtils_replace(data, "\\\"", "\"\"");

    size_t resultLength = csv_parse(this->csvParser, dataWithReplacedQuotes, length,
            SymProtocolDataReader_parseField, SymProtocolDataReader_parseLine, this);

    int rc = length;
    if (resultLength != length) {
        SymLog_error("Error from CSV parser: %s", csv_strerror(csv_error(this->csvParser)));
        rc = 0;
    }

    if (dataWithReplacedQuotes) {
        free(dataWithReplacedQuotes);
    }

    return rc;
}

SymList * SymProtocolDataReader_getBatchesProcessed(SymProtocolDataReader *this) {
    return this->writer->batchesProcessed;
}

void SymProtocolDataReader_close(SymProtocolDataReader *this) {
    csv_fini(this->csvParser, SymProtocolDataReader_parseField, SymProtocolDataReader_parseLine, this);
    this->writer->close(this->writer);
}

void SymProtocolDataReader_destroy(SymProtocolDataReader *this) {
    csv_free(this->csvParser);
    free(this->csvParser);
    this->fields->destroy(this->fields);
    this->batch->destroy(this->batch);
    this->parsedTables->destroy(this->parsedTables);
    free(this);
}

SymProtocolDataReader * SymProtocolDataReader_new(SymProtocolDataReader *this, char *targetNodeId, SymDataWriter *writer) {
    if (this == NULL) {
        this = (SymProtocolDataReader *) calloc(1, sizeof(SymProtocolDataReader));
    }
    this->targetNodeId = targetNodeId;
    this->writer = writer;
    this->csvParser = (struct csv_parser *) calloc(1, sizeof(struct csv_parser));
    this->fields = SymStringArray_newWithSize(NULL, 1024, 1024);
    this->batch = SymBatch_new(NULL);
    this->batch->targetNodeId = SymStringBuilder_copy(targetNodeId);
    this->parsedTables = SymMap_new(NULL, 100);

    SymDataProcessor *super = &this->super;
    super->open = (void *) &SymProtocolDataReader_open;
    super->close = (void *) &SymProtocolDataReader_close;
    super->process = (void *) &SymProtocolDataReader_process;
    super->getBatchesProcessed = (void *) &SymProtocolDataReader_getBatchesProcessed;
    super->destroy = (void *) &SymProtocolDataReader_destroy;
    return this;
}
