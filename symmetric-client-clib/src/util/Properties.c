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
#include "util/Properties.h"

char * SymProperties_get(SymProperties *this, char *key, char *defaultValue) {
    int i;
    for (i = 0; i < this->index; i++) {
        if (strcmp(this->propArray[i].key, key) == 0) {
            return this->propArray[i].value;
        }
    }
    return defaultValue;
}

void SymProperties_put(SymProperties *this, char *key, char *value) {
    this->propArray[this->index].key = key;
    this->propArray[this->index].value = value;
    this->index++;
}

void SymProperties_putAll(SymProperties *this, SymProperties *properties) {
    int i;
    for (i = 0; i < properties->index; i++) {
        this->put(this, properties->propArray[i].key, properties->propArray[i].value);
    }
}

void SymProperties_destroy(SymProperties *this) {
    free(this->propArray);
    free(this);
}

SymProperties * SymProperties_new(SymProperties *this) {
    if (this == NULL) {
        this = (SymProperties *) calloc(1, sizeof(SymProperties));
    }
    this->propArray = (SymProperty *) calloc(255, sizeof(SymProperty));
    this->get = (void *) &SymProperties_get;
    this->put = (void *) &SymProperties_put;
    this->putAll = (void *) &SymProperties_putAll;
    this->destroy = (void *) &SymProperties_destroy;
    return this;
}
