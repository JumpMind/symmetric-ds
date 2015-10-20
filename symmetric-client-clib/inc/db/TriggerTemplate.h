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
#ifndef SYM_TRIGGERTEMPLATE_H
#define SYM_TRIGGERTEMPLATE_H

#include <stdlib.h>
#include "io/data/DataEventType.h"
#include "model/Trigger.h"
#include "model/TriggerHistory.h"
#include "model/Channel.h"
#include "db/model/Table.h"


typedef struct SymTriggerTemplate {
    char * (*createTriggerDDL)(struct SymTriggerTemplate *this, SymDataEventType dml,
            SymTrigger *trigger, SymTriggerHistory *history, SymChannel *channel, char *tablePrefix,
            SymTable *originalTable, char *defaultCatalog, char *defaultSchema);
    long (*toHashedValue)(struct SymTriggerTemplate *this);
    void (*destroy)(struct SymTriggerTemplate *this);
} SymTriggerTemplate;

SymTriggerTemplate * SymTriggerTemplate_new(SymTriggerTemplate *this);

#endif /* SYM_TRIGGERTEMPLATE_H */
