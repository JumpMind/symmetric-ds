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
#ifndef INC_MODEL_TRIGGER_H_
#define INC_MODEL_TRIGGER_H_

#include <stdlib.h>
#include "util/Date.h"


typedef struct SymTrigger {
    char *triggerId;
    char *sourceTableName;
    char *sourceSchemaName;
    char *sourceCatalogName;
    char *channelId;
    char *reloadChannelId;
    unsigned short syncOnUpdate;
    unsigned short syncOnDelete;
    unsigned short syncOnIncomingBatch;
    unsigned short useStreamLogs;
    unsigned short useCaptureLogs;
    unsigned short useCaptureOldData;
    unsigned short *useHandleKeyUpdates;
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
    /**
     * This is a SQL expression that creates a unique id which the sync process
     * can use to 'group' events together and commit together.
     */
    char *txIdExpression;
    char *externalSelect;
    SymDate *createTime;
    SymDate *lastUpdateTime;
    char *lastUpdateBy;

    void (*destroy)(struct SymTrigger *this);
} SymTrigger;

SymTrigger * SymTrigger_new(SymTrigger *this);

#endif /* INC_MODEL_TRIGGER_H_ */
