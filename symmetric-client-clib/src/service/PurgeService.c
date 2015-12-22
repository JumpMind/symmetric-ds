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

typedef struct SymMinMax {
    long min;
    long max;
} SymMinMax;

typedef struct SymNodeBatchRange {
    char *nodeId;
    long minBatchId;
    long maxBatchId;
} SymNodeBatchRange;

static SymMinMax * SymPurgeService_minMaxMapper(SymRow *row) {
    long min = row->getLong(row, "min_id");
    long max = row->getLong(row, "max_id");

    SymMinMax *minMax = (SymMinMax *) calloc(1, sizeof(SymMinMax));
    minMax->min = min;
    minMax->max = max;
    return minMax;
}

static SymNodeBatchRange * SymPurgeService_nodeBatchRangeMapper(SymRow *row) {
    SymNodeBatchRange *nodeBatchRange = (SymNodeBatchRange *) calloc(1, sizeof(SymNodeBatchRange));
    nodeBatchRange->nodeId = row->getStringNew(row, "node_id");
    nodeBatchRange->minBatchId = row->getLong(row, "min_id");
    nodeBatchRange->maxBatchId = row->getLong(row, "max_id");
    return nodeBatchRange;
}

SymMinMax * SymPurgeService_queryForMinMax(SymPurgeService *this, char *sql, SymStringArray *sqlParams) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    SymList *minMaxList = sqlTemplate->query(sqlTemplate, sql, sqlParams, NULL, &error, (void *) SymPurgeService_minMaxMapper);
    if (minMaxList->size > 0) {
        return minMaxList->get(minMaxList, 0);
    } else {
        return NULL;
    }
}

int SymPurgeService_purgeByMinMax(SymPurgeService *this, SymMinMax *minMax, SymMinMaxDeleteSql identifier, SymDate *retentionTime, int maxNumtoPurgeinTx) {

    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    long minId = minMax->min;
    long purgeUpToId = minMax->max;

    int totalCount = 0;
    int totalDeleteStmts = 0;

    SymDate *cutoffTime = retentionTime;

    SymLog_info("About to purge %d", identifier);

    while (minId <= purgeUpToId) {
        totalDeleteStmts++;
        long maxId = minId + maxNumtoPurgeinTx;
        if (maxId > purgeUpToId) {
            maxId = purgeUpToId;
        }

        char* deleteSql = NULL;
        char *minIdString = SymStringUtils_format("%ld", minId);
        char *maxIdString = SymStringUtils_format("%ld", maxId);
        SymStringArray *args = SymStringArray_new(NULL);

        switch (identifier) {
        case SYM_MIN_MAX_DELETE_DATA:
            deleteSql = SYM_SQL_DELETE_DATA;
            args->add(args, minIdString);
            args->add(args, maxIdString);
            args->add(args, cutoffTime->dateTimeString);
            args->add(args, minIdString);
            args->add(args, maxIdString);
            args->add(args, minIdString);
            args->add(args, maxIdString);
            args->add(args, SYM_OUTGOING_BATCH_OK);
            break;
        case SYM_MIN_MAX_DELETE_DATA_EVENT:
            deleteSql = SYM_SQL_DELETE_DATA_EVENT;
            args->add(args, minIdString);
            args->add(args, maxIdString);
            args->add(args, SYM_OUTGOING_BATCH_OK);
            args->add(args, minIdString);
            args->add(args, maxIdString);
            break;
        case SYM_MIN_MAX_DELETE_OUTGOING_BATCH:
            deleteSql = SYM_SQL_DELETE_OUTGOING_BATCH;
            args->add(args, SYM_OUTGOING_BATCH_OK);
            args->add(args, minIdString);
            args->add(args, maxIdString);
            args->add(args, minIdString);
            args->add(args, maxIdString);
            break;
        case SYM_MIN_MAX_DELETE_STRANDED_DATA:
            deleteSql = SYM_SQL_DELETE_STRANDED_DATA;
            args->add(args, minIdString);
            args->add(args, maxIdString);
            args->add(args, cutoffTime->dateTimeString);
            args->add(args, minIdString);
            args->add(args, maxIdString);
            break;
        }

        char *argsString = args->toString(args);
        SymLog_debug("Running the following statement: %s with the following arguments: %s", deleteSql, argsString);
        free(argsString);

        int error;
        int count = sqlTemplate->update(sqlTemplate, deleteSql, args, NULL, &error);
        totalCount += count;

        SymLog_info("Purged %d of (identifier = %d) rows so far using %d statements",
                totalCount, identifier, totalDeleteStmts );

        minId = maxId + 1;
        args->destroy(args);
        free(minIdString);
        free(maxIdString);
    }

    return totalCount;
}

long SymPurgeService_purgeStrandedBatches(SymPurgeService *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, SYM_OUTGOING_BATCH_OK);
    args->add(args, "1");
    args->add(args, SYM_OUTGOING_BATCH_OK);

    int error;
    int rowsAffected = sqlTemplate->update(sqlTemplate, SYM_SQL_UPDATE_STRANDED_BATCHES, args, NULL, &error);

    args->destroy(args);
    return rowsAffected;
}

long SymPurgeService_purgeDataRows(SymPurgeService *this, SymDate *time) {
    SymLog_info("Getting range for data");
    SymMinMax *minMax = SymPurgeService_queryForMinMax(this, SYM_SQL_SELECT_DATA_RANGE, NULL);
    int maxNumOfDataIdsToPurgeInTx =
            this->parameterService->getInt(this->parameterService, SYM_PARAMETER_PURGE_MAX_NUMBER_OF_DATA_IDS, 5000);
    int dataDeletedCount = SymPurgeService_purgeByMinMax(this, minMax,
            SYM_MIN_MAX_DELETE_DATA, time, maxNumOfDataIdsToPurgeInTx);
    int strandedDeletedCount = SymPurgeService_purgeByMinMax(this, minMax,
            SYM_MIN_MAX_DELETE_STRANDED_DATA, time, maxNumOfDataIdsToPurgeInTx);

    return dataDeletedCount + strandedDeletedCount;
}

long SymPurgeService_purgeOutgoingBatch(SymPurgeService *this, SymDate *time) {
    SymLog_info("Getting range for outgoing batch");

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, time->dateTimeString);
    args->add(args, SYM_OUTGOING_BATCH_OK);
    SymMinMax *minMax = SymPurgeService_queryForMinMax(this, SYM_SQL_SELECT_OUTGOING_BATCH_RANGE, args);
    args->destroy(args);

    int maxNumOfBatchIdsToPurgeInTx =
            this->parameterService->getInt(this->parameterService, SYM_PARAMETER_PURGE_MAX_NUMBER_OF_BATCH_IDS, 5000);
    int maxNumOfDataEventsToPurgeInTx =
            this->parameterService->getInt(this->parameterService, SYM_PARAMETER_PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS, 5);

    int dataEventsPurgedCount = SymPurgeService_purgeByMinMax(this, minMax, SYM_MIN_MAX_DELETE_DATA_EVENT,
            time, maxNumOfDataEventsToPurgeInTx);
    int outgoingbatchPurgedCount = SymPurgeService_purgeByMinMax(this, minMax, SYM_MIN_MAX_DELETE_OUTGOING_BATCH,
            time, maxNumOfBatchIdsToPurgeInTx);
    return dataEventsPurgedCount + outgoingbatchPurgedCount;
}

int SymPurgeService_purgeByNodeBatchRangeList(SymPurgeService *this, SymList *nodeBatchRangeList) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    int totalCount = 0;
    int totalDeleteStmts = 0;
    SymLog_info("About to purge incoming batch");

    int i;
    for (i = 0; i < nodeBatchRangeList->size; ++i) {
        SymNodeBatchRange *nodeBatchRange = nodeBatchRangeList->get(nodeBatchRangeList, i);
        int maxNumOfBatchIdsToPurgeInTx =
                this->parameterService->getInt(this->parameterService, SYM_PARAMETER_PURGE_MAX_NUMBER_OF_BATCH_IDS, 5000);

        char *minBatchIdString = SymStringUtils_format("%ld", nodeBatchRange->minBatchId);
        char *maxBatchIdString = SymStringUtils_format("%ld", nodeBatchRange->maxBatchId);

        long minBatchId = nodeBatchRange->minBatchId;
        long purgeUpToBatchId = nodeBatchRange->maxBatchId;

        while (minBatchId <= purgeUpToBatchId) {
            totalDeleteStmts++;
            long maxBatchId = minBatchId + maxNumOfBatchIdsToPurgeInTx;
            if (maxBatchId > purgeUpToBatchId) {
                maxBatchId = purgeUpToBatchId;
            }
            SymStringArray *args = SymStringArray_new(NULL);
            args->add(args, minBatchIdString);
            args->add(args, maxBatchIdString);
            args->add(args, nodeBatchRange->nodeId);
            args->add(args, SYM_OUTGOING_BATCH_OK);

            int error;
            totalCount +=  sqlTemplate->update(sqlTemplate, SYM_SQL_PURGE_INCOMING_BATCH, args, NULL, &error);
            args->destroy(args);
            minBatchId = maxBatchId + 1;
        }

        SymLog_info("Purged %d incoming batch rows so far using %d statements",
                totalCount, totalDeleteStmts );
    }
    SymLog_info("Done purging %d incoming batch rows", totalCount);
    return totalCount;
}

long SymPurgeService_purgeIncomingBatch(SymPurgeService *this, SymDate *time) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, time->dateTimeString);
    args->add(args, SYM_OUTGOING_BATCH_OK);

    SymLog_info("Getting range for incoming batch");
    int error;
    SymList *nodeBatchRangeList = sqlTemplate->query(sqlTemplate, SYM_SQL_SELECT_INCOMING_BATCH_RANGE,
            args, NULL, &error, (void *) SymPurgeService_nodeBatchRangeMapper);
    args->destroy(args);

    int incomingBatchesPurgedCount = SymPurgeService_purgeByNodeBatchRangeList(this, nodeBatchRangeList);
    nodeBatchRangeList->destroy(nodeBatchRangeList);

    return incomingBatchesPurgedCount;
}

long SymPurgeService_purgeIncomingError(SymPurgeService *this) {
    // TODO: support purge incoming error?
//    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
//    SymLog_info("Purging incoming error rows");
//    int error;
//    long rowCount = sqlTemplate.update(getSql("deleteIncomingErrorsSql"));
//    sqlTemplate->update(sqlTemplate, SYM_SQL_DELETE_INCOMING_BATCH, NULL, NULL, &error);
//    SymLog_info("Purged %ld incoming error rows", rowCount);
//    return rowCount;

    return 0;
}

long SymPurgeService_purgeIncomingBeforeDate(SymPurgeService *this, SymDate *retentionCutoff) {
    long purgedRowCount = 0;
    SymLog_info("The incoming purge process is about to run");
    purgedRowCount = SymPurgeService_purgeIncomingBatch(this, retentionCutoff);
    purgedRowCount += SymPurgeService_purgeIncomingError(this);
    SymLog_info("The incoming purge process has completed");

    return purgedRowCount;
}

long SymPurgeService_purgeOutgoingBeforeDate(SymPurgeService *this, SymDate *retentionCutoff) {
    long rowsPurged = 0;
    SymLog_info("The outgoing purge process is about to run for data older than %s",
            retentionCutoff->dateTimeString);
    rowsPurged += SymPurgeService_purgeStrandedBatches(this);
    rowsPurged += SymPurgeService_purgeDataRows(this, retentionCutoff);
    rowsPurged += SymPurgeService_purgeOutgoingBatch(this, retentionCutoff);
    SymLog_info("The outgoing purge process has completed.");

    return rowsPurged;
}

SymDate *SymPurgeService_getRetentionCutoff(SymPurgeService *this) {

    time_t timeInSeconds = time(NULL);
    long timeInMinutes = timeInSeconds / 60;

    int purgeRetentionMinutes =
            this->parameterService->getInt(this->parameterService, SYM_PARAMETER_PURGE_RETENTION_MINUTES, 1440);
    long retentionCutoffMinutes = timeInMinutes - purgeRetentionMinutes;

    SymDate *retentionCutoff = SymDate_newWithTime(retentionCutoffMinutes*60);
    return retentionCutoff;
}

long SymPurgeService_purgeIncoming(SymPurgeService *this) {
    long rowsPurged = 0;
    SymDate *retentionCutoff = SymPurgeService_getRetentionCutoff(this);
    rowsPurged += SymPurgeService_purgeIncomingBeforeDate(this, retentionCutoff);
    retentionCutoff->destroy(retentionCutoff);
    return rowsPurged;
}

long SymPurgeService_purgeOutgoing(SymPurgeService *this) {
    long rowsPurged = 0;
    SymDate *retentionCutoff = SymPurgeService_getRetentionCutoff(this);
    rowsPurged += SymPurgeService_purgeOutgoingBeforeDate(this, retentionCutoff);
    retentionCutoff->destroy(retentionCutoff);
    return rowsPurged;
}

void SymPurgeService_purgeAllIncomingEventsForNode(SymPurgeService *this, char *nodeId) {
    // TODO
}

void SymPurgeService_destroy(SymPurgeService *this) {
    free(this);
}

SymPurgeService * SymPurgeService_new(SymPurgeService *this, SymParameterService *parameterService,
        SymDialect *symmetricDialect, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymPurgeService *) calloc(1, sizeof(SymPurgeService));
    }
    this->parameterService = parameterService;
    this->symmetricDialect = symmetricDialect;
    this->platform = platform;

    this->purgeIncoming = (void *) &SymPurgeService_purgeIncoming;
    this->purgeOutgoing = (void *) &SymPurgeService_purgeOutgoing;
    this->purgeIncomingBeforeDate = (void *) &SymPurgeService_purgeIncomingBeforeDate;
    this->purgeOutgoingBeforeDate = (void *) &SymPurgeService_purgeOutgoingBeforeDate;

    this->destroy = (void *) &SymPurgeService_destroy;
    return this;
}
