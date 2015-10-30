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
#ifndef SYM_SQL_TRANSACTION_H
#define SYM_SQL_TRANSACTION_H

#include <stdio.h>
#include <stdlib.h>
#include "util/List.h"
#include "util/StringArray.h"

typedef struct SymSqlTransaction {
    int (*queryForInt)(struct SymSqlTransaction *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    long (*queryForLong)(struct SymSqlTransaction *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    char * (*queryForString)(struct SymSqlTransaction *this, char *sql, SymStringArray *argss, SymList *sqlTypes, int *error);
    SymList * (*query)(struct SymSqlTransaction *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error, void *callback);
    int (*update)(struct SymSqlTransaction *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    void (*prepare)(struct SymSqlTransaction *this, char *sql, int *error);
    int (*addRow)(struct SymSqlTransaction *this, SymStringArray *args, SymList *sqlTypes, int *error);
    void (*commit)(struct SymSqlTransaction *this);
    void (*rollback)(struct SymSqlTransaction *this);
    void (*close)(struct SymSqlTransaction *this);
    void (*destroy)(struct SymSqlTransaction *this);
} SymSqlTransaction;

SymSqlTransaction * SymSqlTransaction_new(SymSqlTransaction *this);

#endif
