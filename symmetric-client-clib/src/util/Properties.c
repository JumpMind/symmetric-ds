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
        if (strcmp(this->list[i]->key, key) == 0) {
            return this->list[i]->value;
        }
    }
    return defaultValue;
}

void SymProperties_put(SymProperties *this, char *key, char *value) {
    SymProperty *property = (SymProperty *) calloc(1, sizeof(SymProperty));
    property->key = key;
    property->value = value;
    this->list[this->index++] = property;
}

void SymProperties_put_all(SymProperties *this, SymProperties *properties) {
    int i;
    for (i = 0; i < properties->index; i++) {
        this->put(this, properties->list[i]->key, properties->list[i]->value);
    }
}

void SymProperties_destroy(SymProperties *this) {
    while (this->index > 0) {
        free(this->list[--(this->index)]);
    }
    free(this->list);
    free(this);
}

SymProperties * SymProperties_new(SymProperties *this) {
    if (this == NULL) {
        this = (SymProperties *) calloc(1, sizeof(SymProperties));
    }
    this->list = (SymProperty **) calloc(1, sizeof(SymProperty *));
    this->list[0] = (SymProperty *) calloc(255, sizeof(SymProperty));
    this->get = (void *) &SymProperties_get;
    this->put = (void *) &SymProperties_put;
    this->put_all = (void *) &SymProperties_put_all;
    this->destroy = (void *) &SymProperties_destroy;
    return this;
}
