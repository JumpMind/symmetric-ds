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
#include "model/TriggerRouter.h"

unsigned short SymTriggerRouter_isRouted(SymTriggerRouter *this, SymDataEventType dataEventType) {
    switch (dataEventType) {
    case SYM_DATA_EVENT_INSERT:
        return this->router->syncOnInsert;
    case SYM_DATA_EVENT_UPDATE:
        return this->router->syncOnUpdate;
    case SYM_DATA_EVENT_DELETE:
        return this->router->syncOnDelete;
    default:
        return -1;
    }
}

unsigned short SymTriggerRouter_isSame(SymTriggerRouter *this, SymTriggerRouter *triggerRouter) {
    return ((this->trigger == NULL && triggerRouter->trigger == NULL)
            || (this->trigger != NULL && triggerRouter->trigger != NULL && this->trigger->matches(this->trigger, triggerRouter->trigger)))
            && ((this->router == NULL && triggerRouter->router == NULL)
            || (this->router != NULL && triggerRouter->router != NULL && this->router->equals(this->router, triggerRouter->router)));
}

void SymTriggerRouter_destroy(SymTriggerRouter *this) {
    if (this->createTime) {
        this->createTime->destroy(this->createTime);
    }
    if (this->lastUpdateTime) {
        this->lastUpdateTime->destroy(this->lastUpdateTime);
    }
    free(this);
}

SymTriggerRouter * SymTriggerRouter_new(SymTriggerRouter *this) {
    if (this == NULL) {
        this = (SymTriggerRouter *) calloc(1, sizeof(SymTriggerRouter));
    }
    this->enabled = 1;
    this->isRouted = (void *) &SymTriggerRouter_isRouted;
    this->isSame = (void *) &SymTriggerRouter_isSame;
    this->destroy = (void *) &SymTriggerRouter_destroy;
    return this;
}
