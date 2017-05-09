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
#include <db/sqlite/SqliteSqlTemplate.h>
#include "common/Log.h"

static void SymSqliteSqlTemplate_prepare(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error, sqlite3_stmt **stmt) {
	SymLog_debug("Preparing %s", sql);
    int rc = sqlite3_prepare_v2(this->db, sql, -1, stmt, NULL);
    if (rc != SQLITE_OK) {
    	SymLog_error("Failed to prepare query: %s", sql);
    	SymLog_error("SQL Exception (rc=%d): %s", rc, sqlite3_errmsg(this->db));
        *error = rc;
    } else {
        // TODO: do we need to convert to sqlType and bind correctly?

    	SymStringBuilder *buff = SymStringBuilder_new();
    	buff->append(buff, "Binding [");
        int i;
        for (i = 0; args != NULL && i < args->size; i++) {
            // TODO: pass argLengths instead of -1
            sqlite3_bind_text(*stmt, i + 1, args->get(args, i), -1, SQLITE_STATIC);
            if (i > 0) {
            	buff->append(buff, ",");
            }
            buff->appendf(buff, "%s", args->get(args, i));
        }
        buff->append(buff, "]");
        SymLog_debug(buff->toString(buff));
        buff->destroy(buff);
        *error = 0;
    }
}

static SymRow * SymSqliteSqlTemplate_buildRow(sqlite3_stmt *stmt) {
    int i, type, size, columnCount = sqlite3_column_count(stmt);
    SymRow *row = SymRow_new(NULL, columnCount);
    char *name, *value;
    for (i = 0; i < columnCount; i++) {
        name = (char *) sqlite3_column_name(stmt, i);
        type = sqlite3_column_type(stmt, i);
        size = sqlite3_column_bytes(stmt, i) + 1;
        value = (char *) sqlite3_column_text(stmt, i);
        row->put(row, name, value, type, size);
    }
    return row;
}

SymList * SymSqliteSqlTemplate_query(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error, void *map_row(SymRow *row)) {
    sqlite3_stmt *stmt;
    SymSqliteSqlTemplate_prepare(this, sql, args, sqlTypes, error, &stmt);
    SymList *list = SymList_new(NULL);
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        SymRow *row = SymSqliteSqlTemplate_buildRow(stmt);
        void *object = map_row(row);
        list->add(list, object);
        row->destroy(row);
    }

    if (rc != SQLITE_DONE) {
        SymLog_error("Failed to execute query: %s", sql);
        SymLog_error("SQL Exception (rc=%d): %s", rc, sqlite3_errmsg(this->db));
        *error = rc;
    } else {
        *error = 0;
    }
    sqlite3_finalize(stmt);
    return list;
}

SymList * SymSqliteSqlTemplate_queryWithUserData(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error,
        void *map_row(SymRow *row, void *userData), void *userData) {
    sqlite3_stmt *stmt;
    SymSqliteSqlTemplate_prepare(this, sql, args, sqlTypes, error, &stmt);
    SymList *list = SymList_new(NULL);
    int rc;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        SymRow *row = SymSqliteSqlTemplate_buildRow(stmt);
        void *object = map_row(row, userData);
        list->add(list, object);
        row->destroy(row);
    }

    if (rc != SQLITE_DONE) {
        SymLog_error("Failed to execute query: %s", sql);
        SymLog_error("SQL Exception (rc=%d): %s", rc, sqlite3_errmsg(this->db));
        *error = rc;
    } else {
        *error = 0;
    }
    sqlite3_finalize(stmt);
    return list;
}

SymRow * SymSqliteSqlTemplate_queryForList(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error) {
    return (SymRow *) SymSqliteSqlTemplate_query(this, sql, args, sqlTypes, error, (void *) SymRowMapper_mapper);
}

void * SymSqliteSqlTemplate_queryForObject(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error, void *map_row(SymRow *row)) {
    SymList *list = SymSqliteSqlTemplate_query(this, sql, args, sqlTypes, error, map_row);
    void *object = list->get(list, 0);
    list->destroy(list);
    return object;
}

int SymSqliteSqlTemplate_queryForInt(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error) {
    int rc;
    sqlite3_stmt *stmt;
    SymSqliteSqlTemplate_prepare(this, sql, args, sqlTypes, &rc, &stmt);

    int value = 0;
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        value = sqlite3_column_int(stmt, 0);
        *error = 0;
    } else {
    	SymLog_error("Failed to execute query: %s", sql);
    	SymLog_error("SQL Exception (rc=%d): %s", rc, sqlite3_errmsg(this->db));
    	*error = rc;
    }
    sqlite3_finalize(stmt);
    return value;
}

char * SymSqliteSqlTemplate_queryForString(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error) {
    int rc;
    sqlite3_stmt *stmt;
    SymSqliteSqlTemplate_prepare(this, sql, args, sqlTypes, &rc, &stmt);

    char *value = NULL;
    if ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        value = SymStringBuilder_copy((char *) sqlite3_column_text(stmt, 0));
        *error = 0;
    } else if (rc == SQLITE_DONE) {
        SymLog_debug("No result for query: %s", sql);
        // No Results.
    } else {
    	SymLog_error("Failed to execute query: %s", sql);
    	SymLog_error("SQL Exception (rc=%d): %s", rc, sqlite3_errmsg(this->db));
    	*error = rc;
    }
    sqlite3_finalize(stmt);
    return value;
}

int SymSqliteSqlTemplate_queryForLong(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error) {
    char *str = SymSqliteSqlTemplate_queryForString(this, sql, args, sqlTypes, error);
    long value = 0;
    if (str != NULL) {
        value = atol(str);
        free(str);
    }
    return value;
}

int SymSqliteSqlTemplate_update(SymSqliteSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error) {
    int rc;
    sqlite3_stmt *stmt;
    SymSqliteSqlTemplate_prepare(this, sql, args, sqlTypes, &rc, &stmt);

    rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
    	SymLog_error("Failed to execute statement: %s", sql);
    	SymLog_error("SQL Exception (rc=%d): %s", rc, sqlite3_errmsg(this->db));
    	*error = rc;
    } else {
        *error = 0;
    }
    sqlite3_finalize(stmt);
    return sqlite3_changes(this->db);
}

SymSqlTransaction * SymSqliteSqlTemplate_startSqlTransaction(SymSqliteSqlTemplate *this) {
    return (SymSqlTransaction *) SymSqliteSqlTransaction_new(NULL, this);
}

void SymSqliteSqlTemplate_destroy(SymSqliteSqlTemplate *this) {
    free(this);
}

SymSqliteSqlTemplate * SymSqliteSqlTemplate_new(SymSqliteSqlTemplate *this, sqlite3 *db) {
    if (this == NULL) {
        this = (SymSqliteSqlTemplate *) calloc(1, sizeof(SymSqliteSqlTemplate));
    }
    this->db = db;
    SymSqlTemplate *super = (SymSqlTemplate *) this;
    super->queryForInt = (void *) &SymSqliteSqlTemplate_queryForInt;
    super->queryForLong = (void *) &SymSqliteSqlTemplate_queryForLong;
    super->queryForString = (void *) &SymSqliteSqlTemplate_queryForString;
    super->queryForList = (void *) &SymSqliteSqlTemplate_queryForList;
    super->queryForObject = (void *) &SymSqliteSqlTemplate_queryForObject;
    super->query = (void *) &SymSqliteSqlTemplate_query;
    super->queryWithUserData = (void *) &SymSqliteSqlTemplate_queryWithUserData;
    super->update = (void *) &SymSqliteSqlTemplate_update;
    super->startSqlTransaction = (void *) &SymSqliteSqlTemplate_startSqlTransaction;
    super->destroy = (void *) &SymSqliteSqlTemplate_destroy;
    return this;
}
