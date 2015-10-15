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
#include "io/reader/DataReader.h"
#include "io/data/CsvConstants.h"
#include "io/data/CsvData.h"
#include "io/data/Batch.h"
#include "service/DataService.h"
#include "service/TriggerRouterService.h"
#include "common/Log.h"

typedef struct SymExtractDataReader {
    SymDataReader super;
    SymDataService *dataService;
    SymTriggerRouterService *triggerRouterService;

    SymOutgoingBatch *outgoingBatch;
    SymBatch *batch;
    SymTable *targetTable;
    SymTable *sourceTable;
    SymTriggerHistory *lastTriggerHistory;
    char *lastRouterId;

    // TOOD: this should be a cursor instead
    SymList *datas;
    SymIterator *iter;
} SymExtractDataReader;

SymExtractDataReader * SymExtractDataReader_new(SymExtractDataReader *this, SymOutgoingBatch *outgoingBatch, char *sourceNodeId, char *targetNodeId,
        SymDataService *dataService, SymTriggerRouterService *triggerRouterService);

#endif
