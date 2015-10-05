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
#ifndef SYM_CSV_DATA_H
#define SYM_CSV_DATA_H

#include <stdio.h>
#include <stdlib.h>
#include "util/StringArray.h"

typedef enum {
    SYM_DATA_EVENT_INSERT, SYM_DATA_EVENT_UPDATE, SYM_DATA_EVENT_DELETE, SYM_DATA_EVENT_SQL
} SymDataEventType;

typedef enum {
    SYM_CSV_ROW_DATA, SYM_CSV_OLD_DATA, SYM_CSV_PK_DATA
} SymCsvType;

typedef struct SymCsvData {
    SymDataEventType dataEventType;
    SymStringArray *rowData;
    SymStringArray *oldData;
    SymStringArray *pkData;

    void (*reset)(struct SymCsvData *this);
    void (*destroy)(struct SymCsvData *this);
} SymCsvData;

SymCsvData * SymCsvData_new(SymCsvData *this);

#endif
