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

void SymRowEntry_destroy(SymRowEntry *this) {
    free(this);
}

SymRowEntry * SymRowEntry_new(char *value, int sqlType, int size) {
    SymRowEntry *entry = (SymRowEntry *) calloc(1, sizeof(SymRowEntry));
    entry->value = value;
    entry->sqlType = sqlType;
    entry->size = size;
    return entry;
}

void SymRow_put(SymRow *this, char *columnName, void *value, int sqlType, int size) {
    SymRowEntry *entry = SymRowEntry_new(value, sqlType, size);
    this->map->put(this->map, columnName, entry);
}

char * SymRow_getString(SymRow *this, char *columnName) {
    char *value = NULL;
    SymRowEntry *entry = (SymRowEntry *) this->map->get(this->map, columnName);
    if (entry != NULL) {
        value = entry->value;
    }
    return value;
}

char * SymRow_getStringNew(SymRow *this, char *columnName) {
    return SymStringBuilder_copy(this->getString(this, columnName));
}

int SymRow_getInt(SymRow *this, char *columnName) {
    int value = 0;
    char *str = SymRow_getString(this, columnName);
    if (str != NULL) {
        value = atoi(str);
    }
    return value;
}

unsigned short SymRow_getBoolean(SymRow *this, char *columnName) {
    unsigned short value = 0;
    char *str = SymRow_getString(this, columnName);
    if (str != NULL) {
        value = atoi(str);
    }
    return value;
}

long SymRow_getLong(SymRow *this, char *columnName) {
    long value = 0;
    char *str = SymRow_getString(this, columnName);
    if (str != NULL) {
        value = atol(str);
    }
    return value;
}

SymDate * SymRow_getDate(SymRow *this, char *columnName) {
    SymDate *date = NULL;
    char *str = SymRow_getString(this, columnName);
    if (str != NULL) {
        date = SymDate_new(str);
    }
    return date;
}

int SymRow_getSize(SymRow *this, char *columnName) {
    int value = 0;
    SymRowEntry *entry = (SymRowEntry *) this->map->get(this->map, columnName);
    if (entry != NULL) {
        value = entry->size;
    }
    return value;
}

int SymRow_getSqlType(SymRow *this, char *columnName) {
    int value = 0;
    SymRowEntry *entry = (SymRowEntry *) this->map->get(this->map, columnName);
    if (entry != NULL) {
        value = entry->sqlType;
    }
    return value;
}

char * SymRow_stringValue(SymRow *this) {
    char *value = NULL;
    SymStringArray *names = this->map->keys(this->map);
    if (names->size > 0) {
        value = SymStringBuilder_copy(this->map->get(this->map, names->get(names, 0)));
    }
    names->destroy(names);
    return value;
}

int SymRow_intValue(SymRow *this) {
    int value = 0;
    char *str = SymRow_stringValue(this);
    if (str != NULL) {
        value = atoi(str);
        free(str);
    }
    return value;
}

long SymRow_longValue(SymRow *this) {
    long value = 0;
    char *str = SymRow_stringValue(this);
    if (str != NULL) {
        value = atol(str);
        free(str);
    }
    return value;
}

unsigned short SymRow_booleanValue(SymRow *this) {
    unsigned short value = 0;
    char *str = SymRow_stringValue(this);
    if (str != NULL) {
        value = atoi(str);
        free(str);
    }
    return value;
}

SymDate * SymRow_dateValue(SymRow *this) {
    SymDate *value = NULL;
    char *str = SymRow_stringValue(this);
    if (str != NULL) {
        value = SymDate_newWithString(str);
        free(str);
    }
    return value;
}

void SymRow_destroy(SymRow *this) {
    this->map->destroyAll(this->map, (void *) &SymRowEntry_destroy);
    free(this);
}

SymRow * SymRow_new(SymRow *this, int columnCount) {
    if (this == NULL) {
        this = (SymRow *) calloc(1, sizeof(SymRow));
    }
    this->map = SymMap_new(NULL, columnCount);
    this->put = (void *) &SymRow_put;
    this->getInt = (void *) &SymRow_getInt;
    this->getLong = (void *) &SymRow_getLong;
    this->getString = (void *) &SymRow_getString;
    this->getStringNew = (void *) &SymRow_getStringNew;
    this->getBoolean = (void *) &SymRow_getBoolean;
    this->getDate = (void *) &SymRow_getDate;
    this->getSize = (void *) &SymRow_getSize;
    this->getSqlType = (void *) &SymRow_getSqlType;
    this->stringValue = (void *) &SymRow_stringValue;
    this->intValue = (void *) &SymRow_intValue;
    this->longValue = (void *) &SymRow_longValue;
    this->booleanValue = (void *) &SymRow_booleanValue;
    this->dateValue = (void *) &SymRow_dateValue;
    this->destroy = (void *) &SymRow_destroy;
    return this;
}
