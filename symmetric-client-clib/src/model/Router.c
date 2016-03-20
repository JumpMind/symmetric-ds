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
#include "model/Router.h"

unsigned short SymRouter_equals(SymRouter *this, SymRouter *router) {
    if (this && this->routerId && router && router->routerId) {
        return SymStringUtils_equals(this->routerId, router->routerId);
    } else {
        return 0;
    }
}

void SymRouter_destroy(SymRouter *this) {
    free(this);
}

SymRouter * SymRouter_new(SymRouter *this) {
    if (this == NULL) {
        this = (SymRouter *) calloc(1, sizeof(SymRouter));
    }
    this->routerType = "default";
    this->syncOnInsert = 1;
    this->syncOnUpdate = 1;
    this->syncOnDelete = 1;

    this->equals = (void *) &SymRouter_equals;
    this->destroy = (void *) &SymRouter_destroy;
    return this;
}
