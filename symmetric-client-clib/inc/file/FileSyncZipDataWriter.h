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
#ifndef SYM_FILESYNCZIPDATAWRITER_H_
#define SYM_FILESYNCZIPDATAWRITER_H_

#include <stdlib.h>
#include  <zip.h>
#include "io/reader/DataReader.h"
#include "io/writer/DataWriter.h"
#include "util/Map.h"
#include "model/FileSnapshot.h"
#include "io/data/DataContext.h"
#include "service/NodeService.h"

struct FileSyncService;

typedef struct SymFileSyncZipDataWriter {
    SymDataWriter super;
    zip_t * zip;
    char *sourceNodeId;
    SymDataContext *context;

    SymStringBuilder *sb;
    SymBatch *batch;
    SymTable *snapshotTable;
    SymMap *processedTables;
    unsigned short isFirstBatch;
    SymList * /*<SymFileSnapshot>*/ snapshotEvents;
    SymNodeService *nodeService;
    struct SymFileSyncService *fileSyncService;
    long byteCount;
    void (*destroy)(struct SymFileSyncZipDataWriter *this);
} SymFileSyncZipDataWriter;

SymFileSyncZipDataWriter * SymFileSyncZipDataWriter_new(SymFileSyncZipDataWriter *this, char *sourceNodeId);

#endif /* SYM_FILESYNCZIPDATAWRITER_H_ */
