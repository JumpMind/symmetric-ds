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
#include "common/Log.h"

void SymProtocolDataWriter_open(SymProtocolDataWriter *this) {
}

void SymProtocolDataWriter_startBatch(SymProtocolDataWriter *this, SymBatch *batch) {
}

void SymProtocolDataWriter_startTable(SymProtocolDataWriter *this, SymTable *table) {
}

unsigned short SymProtocolDataWriter_write(SymProtocolDataWriter *this, SymCsvData *data) {
    return 0;
}

void SymProtocolDataWriter_endBatch(SymProtocolDataWriter *this, SymBatch *batch) {
}

void SymProtocolDataWriter_endTable(SymProtocolDataWriter *this, SymTable *table) {
}

void SymProtocolDataWriter_close(SymProtocolDataWriter *this) {
}

void SymProtocolDataWriter_destroy(SymProtocolDataWriter *this) {
    free(this);
}

SymProtocolDataWriter * SymProtocolDataWriter_new(SymProtocolDataWriter *this, char *sourceNodeId) {
    if (this == NULL) {
        this = (SymProtocolDataWriter *) calloc(1, sizeof(SymProtocolDataWriter));
    }
    this->sourceNodeId = sourceNodeId;
    SymDataWriter *super = &this->super;
    super->open = (void *) &SymProtocolDataWriter_open;
    super->close = (void *) &SymProtocolDataWriter_close;
    super->startBatch = (void *) &SymProtocolDataWriter_startBatch;
    super->startTable = (void *) &SymProtocolDataWriter_startTable;
    super->write = (void *) &SymProtocolDataWriter_write;
    super->endTable = (void *) &SymProtocolDataWriter_endTable;
    super->endBatch = (void *) &SymProtocolDataWriter_endBatch;
    super->destroy = (void *) &SymProtocolDataWriter_destroy;
    return this;
}
