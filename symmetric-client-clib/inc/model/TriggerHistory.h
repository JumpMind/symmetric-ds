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
#ifndef SYM_TRIGGER_HISTORY_H
#define SYM_TRIGGER_HISTORY_H

#include "util/Date.h"
#include "util/StringArray.h"
#include "util/List.h"
#include "db/model/Column.h"
#include "io/data/DataEventType.h"
#include "common/Log.h"

#define SYM_TRIGGER_REBUILD_REASON_NEW_TRIGGERS "N"
#define SYM_TRIGGER_REBUILD_REASON_TABLE_SCHEMA_CHANGED "S"
#define SYM_TRIGGER_REBUILD_REASON_TABLE_SYNC_CONFIGURATION_CHANGED "C"
#define SYM_TRIGGER_REBUILD_REASON_FORCED "F"
#define SYM_TRIGGER_REBUILD_REASON_TRIGGERS_MISSING "T"
#define SYM_TRIGGER_REBUILD_REASON_TRIGGER_TEMPLATE_CHANGED "E"

typedef struct SymTriggerHistory {
    int triggerHistoryId;
    char *triggerId;
    char *sourceTableName;
    char *sourceSchemaName;
    char *sourceCatalogName;
    SymDate *createTime;
    char *columnNames;
    char *pkColumnNames;
    char *nameForInsertTrigger;
    char *nameForUpdateTrigger;
    char *nameForDeleteTrigger;
    char *errorMessage;
    SymDate *inactiveTime;
    int tableHash;
    long triggerRowHash;
    long triggerTemplateHash;
    char *lastTriggerBuildReason;

    SymStringArray * (*getParsedColumnNames)(struct SymTriggerHistory *this);
    SymStringArray * (*getParsedPkColumnNames)(struct SymTriggerHistory *this);
    char * (*getTriggerNameForDmlType)(struct SymTriggerHistory *this, SymDataEventType type);
    SymList * (*getParsedColumns)(struct SymTriggerHistory *this);
    void (*destroy)(struct SymTriggerHistory *this);
} SymTriggerHistory;

SymTriggerHistory * SymTriggerHistory_new(SymTriggerHistory *this);

SymTriggerHistory * SymTriggerHistory_newWithId(SymTriggerHistory *this, int triggerHistoryId);

void SymTriggerHistory_destroy(SymTriggerHistory *this);

#endif
