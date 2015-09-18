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
}

void SymDefaultDatabaseWriter_close(SymDefaultDatabaseWriter *this) {
    printf("close\n");
}

void SymDefaultDatabaseWriter_start_batch(SymDefaultDatabaseWriter *this, SymBatch *batch) {
    printf("start batch %ld\n", batch->batchId);
}

int SymDefaultDatabaseWriter_start_table(SymDefaultDatabaseWriter *this, SymTable *table) {
    printf("start table %s\n", table->name);
    return 0;
}

void SymDefaultDatabaseWriter_write(SymDefaultDatabaseWriter *this, SymCsvData *data) {
    SymArrayBuilder_print_array(data->rowData, data->sizeRowData);
}

void SymDefaultDatabaseWriter_end_table(SymDefaultDatabaseWriter *this, SymTable *table) {
    printf("end table %s\n", table->name);
}

void SymDefaultDatabaseWriter_end_batch(SymDefaultDatabaseWriter *this, SymBatch *batch) {
    printf("end batch %ld\n", batch->batchId);
}

void SymDefaultDatabaseWriter_destroy(SymDefaultDatabaseWriter *this) {
    free(this);
}

SymDefaultDatabaseWriter * SymDefaultDatabaseWriter_new(SymDefaultDatabaseWriter *this) {
    if (this == NULL) {
        this = (SymDefaultDatabaseWriter *) calloc(1, sizeof(SymDefaultDatabaseWriter));
    }
    SymDataWriter *super = &this->super;
    super->open = (void *) &SymDefaultDatabaseWriter_open;
    super->close = (void *) &SymDefaultDatabaseWriter_close;
    super->start_batch = (void *) &SymDefaultDatabaseWriter_start_batch;
    super->start_table = (void *) &SymDefaultDatabaseWriter_start_table;
    super->write = (void *) &SymDefaultDatabaseWriter_write;
    super->end_table = (void *) &SymDefaultDatabaseWriter_end_table;
    super->end_batch = (void *) &SymDefaultDatabaseWriter_end_batch;
    super->destroy = (void *) &SymDefaultDatabaseWriter_destroy;
    return this;
}
