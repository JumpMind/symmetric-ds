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

typedef struct {
    char *value;
    int sqlType;
    int size;
} SymRowEntry;

typedef struct {
    SymMap *map;
    void (*put)(void *this, char *columnName, char *value, int sqlType, int size);
    char * (*get_string)(void *this, char *columnName);
    int (*get_int)(void *this, char *columnName);
    int (*get_size)(void *this, char *columnName);
    int (*get_sql_type)(void *this, char *columnName);
    void (*destroy)(void *this);
} SymRow;

SymRow * SymRow_new(SymRow *this, int columnCount);

#endif
