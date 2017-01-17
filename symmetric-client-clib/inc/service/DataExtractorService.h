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
#ifndef SYM_DATA_EXTRACTOR_SERVICE_H
#define SYM_DATA_EXTRACTOR_SERVICE_H

#include <stdio.h>
#include "model/Node.h"
#include "model/OutgoingBatch.h"
#include "service/NodeService.h"
#include "service/OutgoingBatchService.h"
#include "service/DataService.h"
#include "service/ParameterService.h"
#include "service/TriggerRouterService.h"
#include "transport/TransportManager.h"
#include "transport/OutgoingTransport.h"
#include "io/data/DataProcessor.h"
#include "io/writer/ProtocolDataWriter.h"
#include "io/reader/ExtractDataReader.h"
#include "db/platform/DatabasePlatform.h"
#include "util/List.h"
#include "common/Log.h"
#include "common/ParameterConstants.h"
#include "io/writer/DataWriter.h"
#include "service/ConfigurationService.h"

typedef struct SymDataExtractorInfo {
    SymOutgoingBatchService *outgoingBatchService;
    SymOutgoingBatches *batches;
    SymNode *sourceNode;
    SymNode *targetNode;
    long bytesSentCount;
    int batchesSentCount;
    long maxBytesToSync;
    SymList *processedBatches;
} SymDataExtractorInfo;

typedef struct SymDataExtractorService {
    SymNodeService *nodeService;
    SymOutgoingBatchService *outgoingBatchService;
    SymDataService *dataService;
    SymTriggerRouterService *triggerRouterService;
    SymParameterService *parameterService;
    SymConfigurationService *configurationService;
    SymDatabasePlatform *platform;
    SymList * (*extract)(struct SymDataExtractorService *this, SymNode *node, SymOutgoingTransport *transport);
    SymList * (*extractOutgoingBatch)(struct SymDataExtractorService *this, SymNode *targetNode, SymDataWriter *dataWriter, SymOutgoingBatch *currentBatch);
    void (*destroy)(struct SymDataExtractorService *this);
} SymDataExtractorService;

SymDataExtractorService * SymDataExtractorService_new(SymDataExtractorService *this, SymNodeService *nodeService, SymOutgoingBatchService *outgoingBatchService,
        SymDataService *dataService, SymTriggerRouterService *triggerRouterService, SymParameterService *parameterService, SymConfigurationService *configurationService, SymDatabasePlatform *platform);

#endif
