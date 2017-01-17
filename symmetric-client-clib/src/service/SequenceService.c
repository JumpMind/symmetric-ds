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
#include "service/SequenceService.h"
#include "common/Log.h"

static SymSequence * sequenceRowMapper(SymRow *row) {
    SymSequence *sequence = SymSequence_new(NULL);
    sequence->createTime = row->getDate(row, "create_time");
    sequence->currentValue = row->getLong(row, "current_value");
    sequence->incrementBy = row->getInt(row, "increment_by");
    sequence->lastUpdateBy = row->getStringNew(row, "last_update_by");
    sequence->lastUpdateTime = row->getDate(row, "last_update_time");
    sequence->maxValue = row->getLong(row, "max_value");
    sequence->minValue = row->getLong(row, "min_value");
    sequence->sequenceName = row->getStringNew(row, "sequence_name");
    sequence->cycle = row->getBoolean(row, "cycle");
    return sequence;
}

static SymMap * getAll(SymSequenceService *this) {
    SymMap *map = SymMap_new(NULL, 5);
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;
    SymList *sequences = sqlTemplate->query(sqlTemplate, SYM_SQL_GET_ALL_SEQUENCE, NULL, NULL, &error, (void *) &sequenceRowMapper);
    SymIterator *iter = sequences->iterator(sequences);
    while (iter->hasNext(iter)) {
        SymSequence *sequence = (SymSequence *) iter->next(iter);
        map->put(map, sequence->sequenceName, sequence);
    }
    iter->destroy(iter);
    sequences->destroy(sequences);
    return map;
}

static long currVal(SymSequenceService *this, SymSqlTransaction *transaction, char *name, int *error) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, name);
    long val = transaction->queryForLong(transaction, SYM_SQL_CURRENT_VALUE, args, NULL, error);
    args->destroy(args);
    return val;
}

long SymSequenceService_currVal(SymSequenceService *this, char * name) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymSqlTransaction *transaction = sqlTemplate->startSqlTransaction(sqlTemplate);
    int error;
    long val = currVal(this, transaction, name, &error);
    if (error == 0) {
        transaction->commit(transaction);
    } else {
        transaction->rollback(transaction);
        val = -1;
    }
    return val;
}

static SymSequence * get(SymSequenceService *this, SymSqlTransaction *transaction, char *name, int *error) {
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, name);

    SymSequence *sequence = NULL;
    SymList *values = transaction->query(transaction, SYM_SQL_GET_SEQUENCE, args, NULL, error, (void *) &sequenceRowMapper);

    if (values->size > 0) {
        sequence = values->get(values, 0);
    }
    args->destroy(args);
    values->destroy(values);
    return sequence;
}

static long nextVal(SymSequenceService *this, SymSqlTransaction *transaction, char *name, int *error) {
    long val = currVal(this, transaction, name, error);
    SymSequence *sequence = this->sequenceDefinitionCache->get(this->sequenceDefinitionCache, name);
    if (sequence == NULL) {
        sequence = get(this, transaction, name, error);
        if (sequence != NULL) {
            this->sequenceDefinitionCache->put(this->sequenceDefinitionCache, name, sequence);
        }
    }

    long nextVal = val + sequence->incrementBy;
    if (nextVal > sequence->maxValue) {
        if (sequence->cycle) {
            nextVal = sequence->minValue;
        }
    } else if (nextVal < sequence->minValue) {
        if (sequence->cycle) {
            nextVal = sequence->maxValue;
        }
    }

    SymStringArray *args = SymStringArray_new(NULL);
    args->addLong(args, nextVal)->add(args, name)->addLong(args, val);

    int updateCount = transaction->update(transaction, SYM_SQL_UPDATE_CURRENT_VALUE, args, NULL, error);
    if (updateCount != 1) {
        nextVal = -1;
    }
    args->destroy(args);

    return nextVal;
}

long SymSequenceService_nextVal(SymSequenceService *this, char *name) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymSqlTransaction *transaction = sqlTemplate->startSqlTransaction(sqlTemplate);
    int error;
    long val = nextVal(this, transaction, name, &error);
    if (error == 0) {
        transaction->commit(transaction);
    } else {
        transaction->rollback(transaction);
        val = -1;
    }
    return val;
}

static void create(SymSequenceService *this, SymSequence *sequence) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, sequence->sequenceName)->addLong(args, sequence->currentValue);
    args->addInt(args, sequence->incrementBy)->addLong(args, sequence->minValue);
    args->addLong(args, sequence->maxValue)->addInt(args, sequence->cycle);
    args->add(args, sequence->lastUpdateBy);

    int error;
    sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_SEQUENCE, args, NULL, &error);

    args->destroy(args);
}

static void initSequence(SymSequenceService *this, char *name, long initialValue) {
    if (initialValue < 1) {
        initialValue = 1;
    }
    SymSequence *sequence = SymSequence_newWithValues(NULL, name, initialValue, 1, 1, 2147483647l, "system", 0);
    create(this, sequence);
    sequence->destroy(sequence);
}

void SymSequenceService_init(SymSequenceService *this) {
    SymMap *sequences = getAll(this);
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    int error;

    if (sequences->get(sequences, SYM_SEQUENCE_OUTGOING_BATCH_LOAD_ID) == NULL) {
        initSequence(this, SYM_SEQUENCE_OUTGOING_BATCH_LOAD_ID, 1);
    }

    if (sequences->get(sequences, SYM_SEQUENCE_OUTGOING_BATCH) == NULL) {
        long maxBatchId = sqlTemplate->queryForLong(sqlTemplate, SYM_SQL_MAX_OUTGOING_BATCH, NULL, NULL, &error);
        initSequence(this, SYM_SEQUENCE_OUTGOING_BATCH, maxBatchId);
    }

    if (sequences->get(sequences, SYM_SEQUENCE_TRIGGER_HIST) == NULL) {
        long maxTriggerHistId = sqlTemplate->queryForLong(sqlTemplate, SYM_SQL_MAX_TRIGGER_HIST, NULL, NULL, &error);
        initSequence(this, SYM_SEQUENCE_TRIGGER_HIST, maxTriggerHistId);
    }

    sequences->destroyAll(sequences, (void *) SymSequence_destroy);

    //if (sequences->get(sequences, SYM_SEQUENCE_EXTRACT_REQ) == NULL) {
    //    long maxRequestId = sqlTemplate->queryForLong(sqlTemplate, SYM_SQL_MAX_EXTRACT_REQUEST, NULL, NULL, &error);
    //    initSequence(this, SYM_SEQUENCE_EXTRACT_REQ, maxRequestId);
    //}
}

void SymSequenceService_destroy(SymSequenceService *this) {
    this->sequenceDefinitionCache->destroyAll(this->sequenceDefinitionCache, (void *) &SymSequence_destroy);
    free(this);
}

SymSequenceService * SymSequenceService_new(SymSequenceService *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymSequenceService *) calloc(1, sizeof(SymSequenceService));
    }
    this->platform = platform;
    this->sequenceDefinitionCache = SymMap_new(NULL, 5);
    this->nextVal = (void *) &SymSequenceService_nextVal;
    this->currVal = (void *) &SymSequenceService_currVal;
    this->init = (void *) &SymSequenceService_init;
    this->destroy = (void *) &SymSequenceService_destroy;
    return this;
}
