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
#ifndef SYM_DML_STATEMENT_H
#define SYM_DML_STATEMENT_H

#include <stdio.h>
#include <stdlib.h>
#include "db/model/Table.h"
#include "db/platform/DatabaseInfo.h"
#include "util/List.h"

typedef enum {
    SYM_DML_TYPE_INSERT, SYM_DML_TYPE_UPDATE, SYM_DML_TYPE_DELETE, SYM_DML_TYPE_SELECT, SYM_DML_TYPE_UNKNOWN
} SymDmlType;

typedef struct SymDmlStatement {
    SymDmlType dmlType;
    SymTable *table;
    SymDatabaseInfo *databaseInfo;
    SymList *nullKeyIndicators;
    char *sql;
    SymList *sqlTypes;
    void (*destroy)(struct SymDmlStatement *this);
} SymDmlStatement;

SymDmlStatement * SymDmlStatement_new(SymDmlStatement *this, SymDmlType dmlType, SymTable *table, SymList *nullKeyIndicators,
        SymDatabaseInfo *databaseInfo);

#endif
