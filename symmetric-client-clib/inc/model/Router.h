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
#ifndef INC_MODEL_ROUTER_H_
#define INC_MODEL_ROUTER_H_

#include <stdlib.h>
#include "model/NodeGroupLink.h"

typedef struct SymRouter {
    char *routerId;
    SymNodeGroupLink *nodeGroupLink;
    char *routerType;

    /**
     * Default to routing all data to all nodes.
     */
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

    void (*destroy)(struct SymRouter *this);
} SymRouter;

SymRouter * SymRouter_new(SymRouter *this);

#endif /* INC_MODEL_ROUTER_H_ */
