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
#include "db/sql/Row.h"

void SymRow_put(SymRow *this, char *columnName, void *value, int sqlType, int size) {
    char *copyValue = NULL;
    if (value != NULL) {
        copyValue = (char *) memcpy(malloc(size + 1), value, size);
        copyValue[size] = '\0';
    }
    SymRowEntry entry = { copyValue, sqlType, size };
    this->map->put(this->map, columnName, &entry, sizeof(SymRowEntry));
}

char * SymRow_get_string(SymRow *this, char *columnName) {
    char *value = NULL;
    SymRowEntry *entry = (SymRowEntry *) this->map->get(this->map, columnName);
    if (entry != NULL) {
        value = entry->value;
    }
    return value;
}

int SymRow_get_int(SymRow *this, char *columnName) {
    int value = 0;
    char *str = SymRow_get_string(this, columnName);
    if (str != NULL) {
        value = atoi(str);
    }
    return value;
}

int SymRow_get_size(SymRow *this, char *columnName) {
    int value = 0;
    SymRowEntry *entry = (SymRowEntry *) this->map->get(this->map, columnName);
    if (entry != NULL) {
        value = entry->size;
    }
    return value;
}

int SymRow_get_sql_type(SymRow *this, char *columnName) {
    int value = 0;
    SymRowEntry *entry = (SymRowEntry *) this->map->get(this->map, columnName);
    if (entry != NULL) {
        value = entry->sqlType;
    }
    return value;
}

void SymRow_destroy(SymRow *this) {
    /*
    int i;
    for (i = 0; i < this->map->size; i++) {
        if (this->map->table[i]) {
            SymRowEntry *entry = (SymRowEntry *) this->map->table[i]->value;
            free(entry->value);
        }
    }
    */
    this->map->destroy(this->map);
    free(this);
}

SymRow * SymRow_new(SymRow *this, int columnCount) {
    if (this == NULL) {
        this = (SymRow *) calloc(1, sizeof(SymRow));
    }
    this->map = SymMap_new(NULL, columnCount);
    this->put = (void *) &SymRow_put;
    this->get_int = (void *) &SymRow_get_int;
    this->get_string = (void *) &SymRow_get_string;
    this->get_size = (void *) &SymRow_get_size;
    this->get_sql_type = (void *) &SymRow_get_sql_type;
    this->destroy = (void *) &SymRow_destroy;
    return this;
}
