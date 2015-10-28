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
#ifndef SYM_ROW_H
#define SYM_ROW_H

#include <stdio.h>
#include <stdlib.h>
#include "util/Map.h"
#include "util/StringBuilder.h"
#include "util/StringArray.h"
#include "util/Date.h"

typedef struct SymRowEntry {
    char *value;
    int sqlType;
    int size;
} SymRowEntry;

typedef struct SymRow {
    SymMap *map;
    void (*put)(struct SymRow *this, char *columnName, char *value, int sqlType, int size);
    char * (*getString)(struct SymRow *this, char *columnName);
    char * (*getStringNew)(struct SymRow *this, char *columnName);
    int (*getInt)(struct SymRow *this, char *columnName);
    long (*getLong)(struct SymRow *this, char *columnName);
    unsigned short (*getBoolean)(struct SymRow *this, char *columnName);
    SymDate * (*getDate)(struct SymRow *this, char *columnName);
    int (*getSize)(struct SymRow *this, char *columnName);
    int (*getSqlType)(struct SymRow *this, char *columnName);
    char * (*stringValue)(struct SymRow *this);
    int (*intValue)(struct SymRow *this);
    long (*longValue)(struct SymRow *this);
    unsigned short (*booleanValue)(struct SymRow *this);
    SymDate * (*dateValue)(struct SymRow *this);
    void (*destroy)(struct SymRow *this);
} SymRow;

SymRow * SymRow_new(SymRow *this, int columnCount);
SymRowEntry * SymRowEntry_new(char *value, int sqlType, int size);
void SymRowEntry_destroy(SymRowEntry *this);

#endif
