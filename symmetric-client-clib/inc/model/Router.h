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
#ifndef SYM_MODEL_ROUTER_H
#define SYM_MODEL_ROUTER_H

#include <stdlib.h>
#include "model/NodeGroupLink.h"
#include "util/StringUtils.h"

typedef struct SymRouter {
    char *routerId;
    SymNodeGroupLink *nodeGroupLink;
    char *routerType;

    char *routerExpression;
    unsigned short syncOnUpdate;
    unsigned short syncOnInsert;
    unsigned short syncOnDelete;
    char *targetCatalogName;
    char *targetSchemaName;
    char *targetTableName;
    unsigned short useSourceCatalogSchema;
    SymDate *createTime;
    SymDate *lastUpdateTime;
    char *lastUpdateBy;

    unsigned short (*equals)(struct SymRouter *this, struct SymRouter *router);
    void (*destroy)(struct SymRouter *this);
} SymRouter;

SymRouter * SymRouter_new(SymRouter *this);
void SymRouter_destroy(SymRouter *this);

#endif
