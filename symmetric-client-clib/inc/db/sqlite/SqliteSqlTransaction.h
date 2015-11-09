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
#ifndef SYM_SQLITE_SQL_TRANSACTION_H
#define SYM_SQLITE_SQL_TRANSACTION_H

#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include "db/sql/SqlTransaction.h"
#include "db/sqlite/SqliteSqlTemplate.h"
#include "util/List.h"

typedef struct SymSqliteSqlTransaction {
    SymSqlTransaction super;
    SymSqlTemplate *sqlTemplate;
    sqlite3 *db;
    sqlite3_stmt *stmt;
    char *sql;
    unsigned short inTransaction;
} SymSqliteSqlTransaction;

SymSqliteSqlTransaction * SymSqliteSqlTransaction_new(SymSqliteSqlTransaction *this, SymSqliteSqlTemplate *sqlTemplate);

#endif
