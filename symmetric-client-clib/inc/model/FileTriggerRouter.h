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
#ifndef SYM_FILETRIGGERROUTER_H_
#define SYM_FILETRIGGERROUTER_H_

#include <stdlib.h>
#include "util/Date.h"
#include "model/Router.h"
#include "model/FileTrigger.h"


typedef struct SymFileTriggerRouter {
    SymFileTrigger *fileTrigger;
    SymRouter *router;
    unsigned short enabled;
    unsigned short initialLoadEnabled;
    char *targetBaseDir;
    char *triggerId;
    char *routerId;
    char *conflictStrategy;
    SymDate *createTime;
    char *lastUpdateBy;
    SymDate *lastUpdateTime;

    void (*destroy)(struct SymFileTriggerRouter *this);
} SymFileTriggerRouter;

SymFileTriggerRouter * SymFileTriggerRouter_new(SymFileTriggerRouter *this);

#endif /* SYM_FILETRIGGERROUTER_H_ */
