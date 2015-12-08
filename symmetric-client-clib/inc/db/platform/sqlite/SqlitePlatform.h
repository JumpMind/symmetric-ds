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
#ifndef SYM_SQLITE_PLATFORM_H
#define SYM_SQLITE_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include "db/platform/DatabasePlatform.h"
#include "db/platform/sqlite/SqliteDdlReader.h"
#include "db/sqlite/SqliteSqlTemplate.h"
#include "util/StringArray.h"

#define SYM_SQLITE_DEFAULT_BUSY_TIMEOUT_MS "30000"

typedef struct SymSqlitePlatform {
    SymDatabasePlatform super;
    sqlite3 *db;
    SymSqlTemplate *sqlTemplate;
} SymSqlitePlatform;

SymSqlitePlatform * SymSqlitePlatform_new(SymSqlitePlatform *this, SymProperties *properties);

#endif
