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
#ifndef SYM_EXTRACT_DATA_READER_H
#define SYM_EXTRACT_DATA_READER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "model/OutgoingBatch.h"
#include "model/Trigger.h"
#include "model/TriggerHistory.h"
#include "model/Router.h"
#include "io/reader/DataReader.h"
#include "io/data/CsvConstants.h"
#include "io/data/CsvData.h"
#include "io/data/Batch.h"
#include "db/platform/DatabasePlatform.h"
#include "db/model/Table.h"
#include "service/DataService.h"
#include "service/TriggerRouterService.h"
#include "util/StringUtils.h"
#include "util/List.h"
#include "common/Log.h"
#include "common/Constants.h"

typedef struct SymExtractDataReader {
    SymDataReader super;
    SymDataService *dataService;
    SymTriggerRouterService *triggerRouterService;
    SymDatabasePlatform *platform;

    char *sourceNodeId;
    char *targetNodeId;
    SymList *outgoingBatches;
    SymIterator *outgoingBatchesIter;
    SymOutgoingBatch *outgoingBatch;
    SymBatch *batch;
    SymTable *targetTable;
    SymTable *sourceTable;
    SymTriggerHistory *lastTriggerHistory;
    char *lastRouterId;
    SymData *nextData;
    unsigned short (*batchProcessed)(SymOutgoingBatch *batch, void *userData);
    void *userData;
    unsigned short keepProcessing;

    // TOOD: this should be a cursor instead
    SymList *dataList;
    SymIterator *dataIter;
} SymExtractDataReader;

SymExtractDataReader * SymExtractDataReader_new(SymExtractDataReader *this, SymList *outgoingBatches, char *sourceNodeId, char *targetNodeId,
        SymDataService *dataService, SymTriggerRouterService *triggerRouterService, SymDatabasePlatform *platform,
        unsigned short *batchProcessed(SymOutgoingBatch *batch, void *userData), void *userData);

#endif
