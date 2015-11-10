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
#include "model/Trigger.h"

SymList * SymTrigger_orderColumnsForTable(SymTrigger *this, SymTable *table) {
    SymList *orderedColumns = SymList_new(NULL);
    if (table != NULL) {
        SymList *pks = this->getSyncKeysColumnsForTable(this, table);
        SymList *cols = table->columns;

        int i;
        for (i = 0; i < pks->size; i++) {
            orderedColumns->add(orderedColumns, pks->get(pks, i));
        }

        for (i = 0; i < cols->size; i++) {
            unsigned short syncKey = 0;
            int j;
            for (j = 0; j < pks->size; j++) {
                char *columnName = ((SymColumn*)cols->get(cols, i))->name;
                char *pkName = ((SymColumn*)pks->get(pks, j))->name;
                if (SymStringUtils_equals(columnName, pkName)) {
                    syncKey = 1;
                    break;
                }
            }
            if (!syncKey) {
                orderedColumns->add(orderedColumns, cols->get(cols, i));
            }
        }

        pks->destroy(pks);
        // TODO filterExcludedColumns
    }
    return orderedColumns;
}

SymStringArray * SymTrigger_getSyncKeyNamesAsList(SymTrigger *this) {
    if (SymStringUtils_isNotBlank(this->syncKeyNames)) {
        SymStringArray *columnNames = SymStringArray_split(this->syncKeyNames, ",");
        int i;
        for (i = 0; i < columnNames->size; ++i) {
            char *toLowerCase = SymStringUtils_toLowerCase(columnNames->get(columnNames, i));
            char *trimmed = SymStringUtils_trim(toLowerCase);
            columnNames->add(columnNames, trimmed);
            free(toLowerCase);
            free(trimmed);
        }
        return columnNames;
    }
    else {
        return SymStringArray_new(NULL);
    }
}

SymList * SymTrigger_getSyncKeysColumnsForTable(SymTrigger *this, SymTable *table) {
    SymStringArray *syncKeys = SymTrigger_getSyncKeyNamesAsList(this);
    SymList *columns = NULL;
    if (syncKeys->size > 0) {
        int i;
        for (i = 0; i < syncKeys->size; ++i) {
            char *syncKey = syncKeys->get(syncKeys, i);
            SymColumn *col = table->findColumn(table, syncKey, 0);
            if (col != NULL) {
                columns->add(columns, col);
            } else {
                SymLog_error("The sync key column '%s' was specified for the '%s' trigger but was not found in the table", syncKey, this->triggerId);
            }
        }

        if (columns->size == 0) {
            columns = table->getPrimaryKeyColumns(table);
        }
    }
    else {
        columns = table->getPrimaryKeyColumns(table);
    }

    syncKeys->destroy(syncKeys);
    return columns;
}

long SymTrigger_toHashedValue(SymTrigger *this) {

    long hashedValue = this->triggerId != NULL ? SymStringBuilder_hashCode(this->triggerId) : 0;
    if (NULL != this->sourceTableName) {
        hashedValue += SymStringBuilder_hashCode(this->sourceTableName);
    }

    if (NULL != this->channelId) {
        hashedValue += SymStringBuilder_hashCode(this->channelId);
    }

    if (NULL != this->sourceSchemaName) {
        hashedValue += SymStringBuilder_hashCode(this->sourceSchemaName);
    }

    if (NULL != this->sourceCatalogName) {
        hashedValue += SymStringBuilder_hashCode(this->sourceCatalogName);
    }

    hashedValue += this->syncOnUpdate ? SymStringBuilder_hashCode("syncOnUpdate") : 0;
    hashedValue += this->syncOnInsert ? SymStringBuilder_hashCode("syncOnInsert") : 0;
    hashedValue += this->syncOnDelete ? SymStringBuilder_hashCode("syncOnDelete") : 0;
    hashedValue += this->syncOnIncomingBatch ? SymStringBuilder_hashCode("syncOnIncomingBatch") : 0;
    hashedValue += this->useStreamLobs ? SymStringBuilder_hashCode("useStreamLobs") : 0;
    hashedValue += this->useCaptureLobs ? SymStringBuilder_hashCode("useCaptureLobs") : 0;
    hashedValue += this->useCaptureOldData ? SymStringBuilder_hashCode("useCaptureOldData") : 0;
    hashedValue += this->useHandleKeyUpdates ? SymStringBuilder_hashCode("useHandleKeyUpdates") : 0;

    if (NULL != this->nameForInsertTrigger) {
        hashedValue += SymStringBuilder_hashCode(this->nameForInsertTrigger);
    }

    if (NULL != this->nameForUpdateTrigger) {
        hashedValue += SymStringBuilder_hashCode(this->nameForUpdateTrigger);
    }

    if (NULL != this->nameForDeleteTrigger) {
        hashedValue += SymStringBuilder_hashCode(this->nameForDeleteTrigger);
    }

    if (NULL != this->syncOnUpdateCondition) {
        hashedValue += SymStringBuilder_hashCode(this->syncOnUpdateCondition);
    }

    if (NULL != this->syncOnInsertCondition) {
        hashedValue += SymStringBuilder_hashCode(this->syncOnInsertCondition);
    }

    if (NULL != this->syncOnDeleteCondition) {
        hashedValue += SymStringBuilder_hashCode(this->syncOnDeleteCondition);
    }

    if (NULL != this->customOnUpdateText) {
        hashedValue += SymStringBuilder_hashCode(this->customOnUpdateText);
    }

    if (NULL != this->customOnInsertText) {
        hashedValue += SymStringBuilder_hashCode(this->customOnInsertText);
    }

    if (NULL != this->customOnDeleteText) {
        hashedValue += SymStringBuilder_hashCode(this->customOnDeleteText);
    }

    if (NULL != this->excludedColumnNames) {
        hashedValue += SymStringBuilder_hashCode(this->excludedColumnNames);
    }

    if (NULL != this->externalSelect) {
        hashedValue += SymStringBuilder_hashCode(this->externalSelect);
    }

    if (NULL != this->txIdExpression) {
        hashedValue += SymStringBuilder_hashCode(this->txIdExpression);
    }

    if (NULL != this->syncKeyNames) {
        hashedValue += SymStringBuilder_hashCode(this->syncKeyNames);
    }

    return hashedValue;
}

unsigned short SymTrigger_matches(SymTrigger *this, SymTrigger *trigger) {
    return SymStringUtils_equals(this->sourceCatalogName, trigger->sourceCatalogName)
            && SymStringUtils_equals(this->sourceSchemaName, trigger->sourceSchemaName)
            && SymStringUtils_equalsIgnoreCase(this->sourceTableName, trigger->sourceTableName);
}

unsigned short SymTrigger_hasChangedSinceLastTriggerBuild(SymTrigger *this, SymDate *lastTriggerBuildTime) {
    return lastTriggerBuildTime == NULL || this->lastUpdateTime == NULL
            || lastTriggerBuildTime->before(lastTriggerBuildTime, this->lastUpdateTime);
}

void SymTrigger_destroy(SymTrigger *this) {
    if (this->createTime) {
        free(this->createTime);
    }
    if (this->lastUpdateTime) {
        free(this->lastUpdateTime);
    }

    free(this);
}

SymTrigger * SymTrigger_new(SymTrigger *this) {
    if (this == NULL) {
        this = (SymTrigger *) calloc(1, sizeof(SymTrigger));
    }
    this->orderColumnsForTable = (void *) &SymTrigger_orderColumnsForTable;
    this->getSyncKeysColumnsForTable = (void *) &SymTrigger_getSyncKeysColumnsForTable;
    this->toHashedValue = (void *) &SymTrigger_toHashedValue;
    this->hasChangedSinceLastTriggerBuild = (void *) &SymTrigger_hasChangedSinceLastTriggerBuild;
    this->matches = (void *) &SymTrigger_matches;
    this->destroy = (void *) &SymTrigger_destroy;
    return this;
}
