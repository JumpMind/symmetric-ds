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
}

SymBatch * SymExtractDataReader_nextBatch(SymExtractDataReader *this) {
    return NULL;
}

SymTable * SymExtractDataReader_nextTable(SymExtractDataReader *this) {
    return NULL;
}

SymCsvData * SymExtractDataReader_nextData(SymExtractDataReader *this) {
    return NULL;
}

void SymExtractDataReader_close(SymExtractDataReader *this) {
}

void SymExtractDataReader_destroy(SymExtractDataReader *this) {
    free(this);
}

SymExtractDataReader * SymExtractDataReader_new(SymExtractDataReader *this, SymOutgoingBatch *outgoingBatch, char *sourceNodeId, char *targetNodeId) {
    if (this == NULL) {
        this = (SymExtractDataReader *) calloc(1, sizeof(SymExtractDataReader));
    }
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
