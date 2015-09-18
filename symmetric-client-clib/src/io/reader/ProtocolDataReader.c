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

static void SymProtocolDataReader_parse_field(void *data, size_t size, void *userData) {
    SymProtocolDataReader *this = (SymProtocolDataReader *) userData;
    this->fields->addn(this->fields, data, size);
}

static void SymProtocolDataReader_parse_line(int eol, void *userData) {
    SymProtocolDataReader *this = (SymProtocolDataReader *) userData;
    SymBatch *batch = this->batch;
    SymTable *table = this->table;
    SymCsvData *csvData = this->csvData;
    SymArrayBuilder *fields = this->fields;

    if (fields->size > 0) {
        char *token = fields->get(fields, 0);
        if (strcmp(token, SYM_CSV_INSERT) == 0) {
            char **rowData = fields->to_array_range(fields, 1, fields->size);
            int sizeRowData = fields->size - 1;
            csvData->reset(csvData);
            csvData->dataEventType = SYM_DATA_EVENT_INSERT;
            csvData->set_array(&csvData->rowData, &csvData->sizeRowData, rowData, sizeRowData);
            this->writer->write(this->writer, csvData);
            SymArrayBuilder_destroy_array(rowData, sizeRowData);
        } else if (strcmp(token, SYM_CSV_OLD) == 0) {
            csvData->reset(csvData);
            char **oldData = fields->to_array_range(fields, 1, fields->size);
            int sizeOldData = fields->size - 1;
            csvData->set_array(&csvData->oldData, &csvData->sizeOldData, oldData, sizeOldData);
            SymArrayBuilder_destroy_array(oldData, sizeOldData);
        } else if (strcmp(token, SYM_CSV_UPDATE) == 0) {
            char **rowData = fields->to_array_range(fields, 1, fields->size);
            int sizeRowData = fields->size - 1;
            char **pkData = fields->to_array_range(fields, 1, table->sizeKeys + 1);
            int sizePkData = table->sizeKeys;
            csvData->dataEventType = SYM_DATA_EVENT_UPDATE;
            csvData->set_array(&csvData->rowData, &csvData->sizeRowData, rowData, sizeRowData);
            csvData->set_array(&csvData->pkData, &csvData->sizePkData, pkData, sizePkData);
            this->writer->write(this->writer, csvData);
            SymArrayBuilder_destroy_array(rowData, sizeRowData);
            SymArrayBuilder_destroy_array(pkData, sizePkData);
        } else if (strcmp(token, SYM_CSV_DELETE) == 0) {
            char **pkData = fields->to_array_range(fields, 1, fields->size);
            int sizePkData = fields->size - 1;
            csvData->reset(csvData);
            csvData->dataEventType = SYM_DATA_EVENT_DELETE;
            csvData->set_array(&csvData->pkData, &csvData->sizePkData, pkData, sizePkData);
            csvData->sizePkData = sizePkData;
            this->writer->write(this->writer, csvData);
            SymArrayBuilder_destroy_array(pkData, sizePkData);
        } else if (strcmp(token, SYM_CSV_CATALOG) == 0) {
            table->set(&table->catalog, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_SCHEMA) == 0) {
            table->set(&table->schema, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_TABLE) == 0) {
            table->set(&table->name, fields->get(fields, 1));
            this->writer->start_table(this->writer, table);
        } else if (strcmp(token, SYM_CSV_KEYS) == 0) {
            char **keys = fields->to_array_range(fields, 1, fields->size);
            int sizeKeys = fields->size - 1;
            table->set_array(&table->keys, &table->sizeKeys, keys, sizeKeys);
            SymArrayBuilder_destroy_array(keys, sizeKeys);
        } else if (strcmp(token, SYM_CSV_COLUMNS) == 0) {
            char **columns = fields->to_array_range(fields, 1, fields->size);
            int sizeColumns = fields->size - 1;
            table->set_array(&table->columns, &table->sizeColumns, columns, sizeColumns);
            SymArrayBuilder_destroy_array(columns, sizeColumns);
        } else if (strcmp(token, SYM_CSV_NODEID) == 0) {
            batch->set(&batch->sourceNodeId, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_CHANNEL) == 0) {
            batch->set(&batch->channelId, fields->get(fields, 1));
        } else if (strcmp(token, SYM_CSV_BATCH) == 0) {
            batch->batchId = atol(fields->get(fields, 1));
            this->writer->start_batch(this->writer, batch);
        } else if (strcmp(token, SYM_CSV_COMMIT) == 0) {
            this->writer->end_batch(this->writer, batch);
        } else if (strcmp(token, SYM_CSV_IGNORE) == 0) {
            batch->isIgnore = 1;
        } else if (strcmp(token, SYM_CSV_SQL) == 0) {
            char **rowData = fields->to_array_range(fields, 1, fields->size);
            int sizeRowData = fields->size - 1;
            csvData->reset(csvData);
            csvData->dataEventType = SYM_DATA_EVENT_SQL;
            csvData->set_array(&csvData->rowData, &csvData->sizeRowData, rowData, sizeRowData);
            this->writer->write(this->writer, csvData);
            SymArrayBuilder_destroy_array(rowData, sizeRowData);
        }
    }
    fields->reset(fields);
}

void SymProtocolDataReader_open(SymProtocolDataReader *this) {
    csv_init(this->csvParser, 0);
    this->writer->open(this->writer);
}

size_t SymProtocolDataReader_process(SymProtocolDataReader *this, char *data, size_t size, size_t count) {
    size_t length = size * count;
    if (csv_parse(this->csvParser, data, length, SymProtocolDataReader_parse_field, SymProtocolDataReader_parse_line, this) != length) {
        fprintf(stderr, "Error from CSV parser: %s\n", csv_strerror(csv_error(this->csvParser)));
        return 0;
    }
    return length;
}

void SymProtocolDataReader_close(SymProtocolDataReader *this) {
    csv_fini(this->csvParser, SymProtocolDataReader_parse_field, SymProtocolDataReader_parse_line, this);
    this->writer->close(this->writer);
}

void SymProtocolDataReader_destroy(SymProtocolDataReader *this) {
    csv_free(this->csvParser);
    free(this->csvParser);
    this->fields->destroy(this->fields);
    this->batch->destroy(this->batch);
    this->table->destroy(this->table);
    this->csvData->destroy(this->csvData);
    free(this);
}

SymProtocolDataReader * SymProtocolDataReader_new(SymProtocolDataReader *this, char *targetNodeId, SymDataWriter *writer) {
    if (this == NULL) {
        this = (SymProtocolDataReader *) calloc(1, sizeof(SymProtocolDataReader));
    }
    this->targetNodeId = targetNodeId;
    this->writer = writer;
    this->csvParser = (struct csv_parser *) calloc(1, sizeof(struct csv_parser));
    this->fields = SymArrayBuilder_new();
    this->batch = SymBatch_new(NULL);
    this->batch->set(&(this->batch->targetNodeId), targetNodeId);
    this->table = SymTable_new(NULL);
    this->csvData = SymCsvData_new(NULL);

    SymDataReader *super = &this->super;
    super->open = (void *) &SymProtocolDataReader_open;
    super->close = (void *) &SymProtocolDataReader_close;
    super->process = (void *) &SymProtocolDataReader_process;
    super->destroy = (void *) &SymProtocolDataReader_destroy;
    return this;
}
