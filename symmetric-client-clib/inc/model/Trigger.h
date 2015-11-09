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
#ifndef SYM_MODEL_TRIGGER_H
#define SYM_MODEL_TRIGGER_H

#include <stdlib.h>
#include "util/Date.h"
#include "db/model/Table.h"
#include "util/List.h"
#include "util/StringArray.h"
#include "util/StringUtils.h"
#include "common/Log.h"


typedef struct SymTrigger {
    char *triggerId;
    char *sourceTableName;
    char *sourceSchemaName;
    char *sourceCatalogName;
    char *channelId;
    char *reloadChannelId;
    unsigned short syncOnUpdate;
    unsigned short syncOnInsert;
    unsigned short syncOnDelete;
    unsigned short syncOnIncomingBatch;
    unsigned short useStreamLobs;
    unsigned short useCaptureLobs;
    unsigned short useCaptureOldData;
    unsigned short useHandleKeyUpdates;
    char *nameForInsertTrigger;
    char *nameForUpdateTrigger;
    char *nameForDeleteTrigger;
    char *syncOnUpdateCondition;
    char *syncOnInsertCondition;
    char *syncOnDeleteCondition;
    char *channelExpression;
    char *customOnUpdateText;
    char *customOnInsertText;
    char *customOnDeleteText;
    char *excludedColumnNames;
    char *syncKeyNames;
    /**
     * This is a SQL expression that creates a unique id which the sync process
     * can use to 'group' events together and commit together.
     */
    char *txIdExpression;
    char *externalSelect;
    SymDate *createTime;
    SymDate *lastUpdateTime;
    char *lastUpdateBy;

    SymList * (*orderColumnsForTable)(struct SymTrigger *this, SymTable *table);
    SymList * (*getSyncKeysColumnsForTable)(struct SymTrigger *this, SymTable *table);
    unsigned short (*hasChangedSinceLastTriggerBuild)(struct SymTrigger *this, SymDate *lastTriggerBuildTime);
    unsigned short (*matches)(struct SymTrigger *this, struct SymTrigger *trigger);
    long (*toHashedValue)(struct SymTrigger *this);
    void (*destroy)(struct SymTrigger *this);
} SymTrigger;

SymTrigger * SymTrigger_new(SymTrigger *this);
void SymTrigger_destroy(SymTrigger *this);

#endif /* SYM_MODEL_TRIGGER_H */
