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
#include "db/platform/sqlite/SqlitePlatform.h"
#include "common/Log.h"

static int SymSqlitePlatform_tableExistsCallback(void *exists, int argc, char **argv, char **columName) {
    *((int *) exists) = argc > 0;
    return 0;
}

int SymSqlitePlatform_tableExists(SymDatabasePlatform *super, char *tableName) {
    int exists = 0;
    char sql[100];
    SymSqlitePlatform *this = (SymSqlitePlatform *) super;
    snprintf(sql, 100, "pragma table_info('%s')", tableName);
    sqlite3_exec(this->db, sql, &SymSqlitePlatform_tableExistsCallback, &exists, NULL);
    return exists;
}

int SymSqlitePlatform_executeSql(SymDatabasePlatform *super,
        const char *sql, int (*callback)(void*, int, char **, char **),
        void *arg, char **errorMessage) {
    SymSqlitePlatform *this = (SymSqlitePlatform *) super;
    return sqlite3_exec(this->db, sql, callback, arg, errorMessage);
}

SymSqliteSqlTemplate * SymSqlitePlatform_getSqlTemplate(SymSqlitePlatform *this) {
    return (SymSqliteSqlTemplate *) this->sqlTemplate;
}

void SymSqlitePlatform_destroy(SymDatabasePlatform *super) {
	SymLog_info("Closing SQLite database");
    SymSqlitePlatform *this = (SymSqlitePlatform *) super;
    sqlite3_close(this->db);
    this->sqlTemplate->destroy(this->sqlTemplate);
//	  free(super->ddlReader);
    free(this);
}

SymSqlitePlatform * SymSqlitePlatform_new(SymSqlitePlatform *this, SymProperties *properties) {
    if (this == NULL) {
        this = (SymSqlitePlatform *) calloc(1, sizeof(SymSqlitePlatform));
    }
    SymDatabasePlatform *super = SymDatabasePlatform_new(&this->super);
    super->databaseInfo.catalogSeparator = ".";
    super->databaseInfo.schemaSeparator = ".";
    super->databaseInfo.delimiterToken = "\"";
    super->ddlReader = (SymDdlReader *) SymSqliteDdlReader_new(NULL, (SymDatabasePlatform *) this);
    super->name = SYM_DATABASE_SQLITE;
    super->version = (char *) sqlite3_libversion();
    super->executeSql = (void *) &SymSqlitePlatform_executeSql;
    super->tableExists = (void *) &SymSqlitePlatform_tableExists;
    super->getSqlTemplate = (void *) &SymSqlitePlatform_getSqlTemplate;
    super->destroy = (void *) &SymSqlitePlatform_destroy;

    SymLog_info("The IDatabasePlatform being used is SymSqlitePlatform");

    char filename[256];
    char *url = properties->get(properties, SYM_PARAMETER_DB_URL, SYM_DATABASE_SQLITE);
    url += strlen(SYM_DATABASE_SQLITE) + 1;
    strncpy(filename, url, 256);
    SymLog_info("Opening SQLite database at %s", filename);

    if (sqlite3_open_v2(filename, &this->db,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL)) {
    	SymLog_error("Can't open database: %s", sqlite3_errmsg(this->db));
        sqlite3_close(this->db);
        return NULL;
    }

    int busyTimeout = atoi(properties->get(properties, SYM_PARAMETER_SQLITE_BUSY_TIMEOUT_MS, SYM_SQLITE_DEFAULT_BUSY_TIMEOUT_MS));

    sqlite3_busy_timeout(this->db, busyTimeout);

    this->sqlTemplate = (SymSqlTemplate *) SymSqliteSqlTemplate_new(NULL, this->db);

    SymLog_info("Detected database '%s', version '%s'", super->name, super->version);

    char *initSql = properties->get(properties, SYM_PARAMETER_SQLITE_INIT_SQL, NULL);
    if (SymStringUtils_isNotBlank(initSql)) {
        int error, i;
        SymLog_debug("Initializing database with '%s'", initSql);
        SymStringArray *array = SymStringArray_split(initSql, ";");
        for (i = 0; i < array->size; i++) {
            char *sql = array->get(array, i);
            if (SymStringUtils_isNotBlank(sql)) {
                this->sqlTemplate->update(this->sqlTemplate, sql, NULL, NULL, &error);
            }
        }
    }

    return this;
}
