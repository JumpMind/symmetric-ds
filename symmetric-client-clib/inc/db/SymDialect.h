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
#ifndef SYM_DIALECT_H
#define SYM_DIALECT_H

#include <stdio.h>
#include <stdlib.h>
#include <util/Properties.h>
#include "db/platform/DatabasePlatform.h"
#include "model/Trigger.h"
#include "model/Channel.h"
#include "model/TriggerHistory.h"
#include "io/data/DataEventType.h"
#include "db/TriggerTemplate.h"

typedef struct SymDialect {
    SymDatabasePlatform *platform;
    SymTriggerTemplate *triggerTemplate;
    int (*initTablesAndDatabaseObjects)(struct SymDialect *this);
    int (*dropTablesAndDatabaseObjects)(struct SymDialect *this);
    void (*disableSyncTriggers)(struct SymDialect *this, SymSqlTransaction *transaction, char *nodeId);
    void (*enableSyncTriggers)(struct SymDialect *this, SymSqlTransaction *transaction);
    int (*createTrigger)(struct SymDialect *this, SymDataEventType dml, SymTrigger *trigger,
        SymTriggerHistory *hist, SymChannel *channel, char* tablePrefix, SymTable *table);
    int (*removeTrigger)(struct SymDialect *this, char *sqlBuffer,
            char *catalogName, char *schema, char *tableName, char *triggerName);
    unsigned short (*doesTriggerExist)(struct SymDialect *this,
            char *catalogName, char *schema, char *tableName, char *triggerName);
    int (*getInitialLoadSql)(struct SymDialect *this);
    void (*destroy)(struct SymDialect *this);
} SymDialect;

SymDialect * SymDialect_new(SymDialect *this, SymDatabasePlatform *platform);

#endif
