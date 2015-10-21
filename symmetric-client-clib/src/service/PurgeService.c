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
#include "service/PurgeService.h"
#include "common/Log.h"

long SymPurgeService_purgeIncomingBeforeDate(SymPurgeService *this, SymDate *retentionCutoff) {
    // TODO
    return 0;
}

long SymPurgeService_purgeOutgoingBeforeDate(SymPurgeService *this, SymDate *retentionCutoff) {
    // TODO
    return 0;
}

long SymPurgeService_purgeIncoming(SymPurgeService *this) {
    // TODO
    return 0;
}

long SymPurgeService_purgeOutgoing(SymPurgeService *this) {
    // TODO
    return 0;
}

long SymPurgeService_purgeOutgoingBatch(SymPurgeService *this, SymDate *time) {
    // TODO
    return 0;
}

long SymPurgeService_purgeStrandedBatches(SymPurgeService *this) {
    // TODO
    return 0;
}

long SymPurgeService_purgeDataRows(SymPurgeService *this, SymDate *time) {
    // TODO
    return 0;
}

long* SymPurgeService_queryForMinMax(SymPurgeService *this, char *sql, ...) {
    // TODO
    return 0;
}

int SymPurgeService_purgeByMinMax(SymPurgeService *this, long *minMax, SymMinMaxDeleteSql *identifier, SymDate *retentionTime, int maxNumtoPurgeinTx) {
    // TODO
    return 0;
}
//
//long SymPurgeService_purgeIncoming(SymPurgeService *this, SymDate *retentionCutoff, unsigned short force) {
//    // TODO
//    return 0;
//}

long SymPurgeService_purgeIncomingError(SymPurgeService *this) {
    // TODO
    return 0;
}

long SymPurgeService_purgeIncomingBatch(SymPurgeService *this, SymDate *time) {
    // TODO
    return 0;
}

int SymPurgeService_purgeByNodeBatchRangeList(SymPurgeService *this, SymList *nodeBatchRangeList) {
    // TODO
    return 0;
}

void SymPurgeService_purgeStats(SymPurgeService *this, unsigned short force) {
    // TODO
}

//char * SymNodeBatchRange_getNodeId(SymNodeBatchRange *this) {
//    // TODO
//    return 0;
//}
//
//long SymNodeBatchRange_getMaxBatchId(SymNodeBatchRange *this) {
//    // TODO
//    return 0;
//}
//
//long SymNodeBatchRange_getMinBatchId(SymNodeBatchRange *this) {
//    // TODO
//    return 0;
//}

void SymPurgeService_purgeAllIncomingEventsForNode(SymPurgeService *this, char *nodeId) {
    // TODO
}

void SymPurgeService_destroy(SymPurgeService *this) {
    free(this);
}

SymPurgeService * SymPurgeService_new(SymPurgeService *this, SymParameterService *parameterService,
        SymDialect *symmetricDialect) {
    if (this == NULL) {
        this = (SymPurgeService *) calloc(1, sizeof(SymPurgeService));
    }
    this->parameterService = parameterService;
    this->symmetricDialect = symmetricDialect;

    this->destroy = (void *) &SymPurgeService_destroy;
    return this;
}
