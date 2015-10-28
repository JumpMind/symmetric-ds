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
#ifndef SYM_DATABASE_PLATFORM_H
#define SYM_DATABASE_PLATFORM_H

#include <stdio.h>
#include <stdlib.h>
#include <util/Properties.h>
#include "service/ParameterService.h"
#include "db/model/Table.h"
#include "db/platform/DdlReader.h"
#include "db/sql/SqlTemplate.h"
#include "db/platform/DatabaseInfo.h"

#define SYM_DATABASE_SQLITE "sqlite"
#define SYM_DATABASE_UNDEFINED "undefined"

typedef struct SymDatabasePlatform {
    SymProperties *properties;
    char *name;
    char *version;
    char *defaultCatalog;
    char *defaultSchema;
    SymDatabaseInfo databaseInfo;
    SymDdlReader *ddlReader;
    int (*executeSql)(struct SymDatabasePlatform *this, const char *sql,
            int (*callback)(void *userData, int sizeColumns, char **columnNames, char **columnValues),
            void *userData, char **errorMessage);
    int (*tableExists)(struct SymDatabasePlatform *this, char *tableName);
    SymSqlTemplate * (*getSqlTemplate)(struct SymDatabasePlatform *this);
    SymTable * (*getTableFromCache)(struct SymDatabasePlatform *this, char *catalog, char *schema, char *tableName, unsigned int forceReread);
    SymTable * (*readTableFromDatabase)(struct SymDatabasePlatform *this, char *catalog, char *schema, char *tableName);
    void (*resetCachedTableModel)(struct SymDatabasePlatform *this);
    SymTable * (*makeAllColumnsPrimaryKeys)(struct SymDatabasePlatform *this, SymTable *table);
    void (*destroy)(void *this);
} SymDatabasePlatform;

SymDatabasePlatform * SymDatabasePlatform_new(SymDatabasePlatform *this);

#endif
