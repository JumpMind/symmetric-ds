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

typedef struct {
    int (*query_for_int)(void *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    char * (*query_for_string)(void *this, char *sql, SymStringArray *argss, SymList *sqlTypes, int *error);
    void (*query)(void *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error, void *callback);
    int (*update)(void *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    void (*prepare)(void *this, char *sql);
    int (*add_row)(void *this, SymStringArray *args, SymList *sqlTypes);
    void (*commit)(void *this);
    void (*rollback)(void *this);
    void (*close)(void *this);
    void (*destroy)(void *this);
} SymSqlTransaction;

SymSqlTransaction * SymSqlTransaction_new(SymSqlTransaction *this);

#endif
