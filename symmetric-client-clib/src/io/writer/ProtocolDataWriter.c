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
}

size_t SymProtocolDataWriter_process(SymProtocolDataWriter *this,  char *data, size_t size, size_t count) {
    size_t length = size * count;
    return length;
}

void SymProtocolDataWriter_close(SymProtocolDataWriter *this) {
}

void SymProtocolDataWriter_destroy(SymProtocolDataWriter *this) {
    free(this);
}

SymProtocolDataWriter * SymProtocolDataWriter_new(SymProtocolDataWriter *this, char *sourceNodeId, SymDataReader *reader) {
    if (this == NULL) {
        this = (SymProtocolDataWriter *) calloc(1, sizeof(SymProtocolDataWriter));
    }
    this->sourceNodeId = sourceNodeId;
    this->reader = reader;
    SymDataProcessor *super = &this->super;
    super->open = (void *) &SymProtocolDataWriter_open;
    super->close = (void *) &SymProtocolDataWriter_close;
    super->process = (void *) &SymProtocolDataWriter_process;
    super->destroy = (void *) &SymProtocolDataWriter_destroy;
    return this;
}
