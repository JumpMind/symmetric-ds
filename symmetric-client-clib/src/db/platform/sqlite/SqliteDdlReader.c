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
#include "db/platform/sqlite/SqliteDdlReader.h"

static int SymSqliteDdlReader_to_sql_type(char *type) {
    int sqlType;
    if (type == NULL) {
        type = "TEXT";
    }
    if (strncasecmp(type, "INT", 3) == 0) {
        sqlType = SYM_SQL_TYPE_INTEGER;
    } else if (strncasecmp(type, "NUM", 3) == 0) {
        sqlType = SYM_SQL_TYPE_NUMERIC;
    } else if (strncasecmp(type, "BLOB", 4) == 0) {
        sqlType = SYM_SQL_TYPE_BLOB;
    } else if (strncasecmp(type, "CLOB", 4) == 0) {
        sqlType = SYM_SQL_TYPE_CLOB;
    } else if (strncasecmp(type, "FLOAT", 5) == 0) {
        sqlType = SYM_SQL_TYPE_FLOAT;
    } else if (strncasecmp(type, "DOUBLE", 6) == 0) {
        sqlType = SYM_SQL_TYPE_DOUBLE;
    } else if (strncasecmp(type, "REAL", 4) == 0) {
        sqlType = SYM_SQL_TYPE_REAL;
    } else if (strncasecmp(type, "DECIMAL", 7) == 0) {
        sqlType = SYM_SQL_TYPE_DECIMAL;
    } else if (strncasecmp(type, "DATE", 4) == 0) {
        sqlType = SYM_SQL_TYPE_DATE;
    } else if (strncasecmp(type, "TIMESTAMP", 9) == 0) {
        sqlType = SYM_SQL_TYPE_TIMESTAMP;
    } else if (strncasecmp(type, "TIME", 4) == 0) {
        sqlType = SYM_SQL_TYPE_TIME;
    } else {
        sqlType = SYM_SQL_TYPE_VARCHAR;
    }
    return sqlType;
}

static void * SymSqliteDdlReader_column_mapper(SymRow *row) {
    char *name = row->get_string(row, "name");
    int isPrimaryKey = row->get_int(row, "pk");
    SymColumn *column = SymColumn_new(NULL, name, isPrimaryKey);
    column->isRequired = row->get_int(row, "notnull");
    column->sqlType = SymSqliteDdlReader_to_sql_type(row->get_string(row, "type"));
    return column;
}

SymTable * SymSqliteDdlReader_read_table(SymSqliteDdlReader *this, char *catalog, char *schema, char *tableName) {
    SymTable *table = NULL;
    SymSqlTemplate *sqlTemplate = this->platform->get_sql_template(this->platform);
    SymStringBuilder *sql = SymStringBuilder_new_with_string("pragma table_info(");
    sql->append(sql, tableName)->append(sql, ")");
    int error;
    SymList *columns = sqlTemplate->query(sqlTemplate, sql->str, NULL, NULL, &error, SymSqliteDdlReader_column_mapper);
    if (columns && columns->size > 0) {
        table = SymTable_new(NULL);
        table->name = tableName;
        table->columns = columns;
    }
    sql->destroy(sql);
    return table;
}

void SymSqliteDdlReader_destroy(SymSqliteDdlReader *this) {
    SymDdlReader *super = (SymDdlReader *) this;
    super->destroy(super);
    free(this);
}

SymSqliteDdlReader * SymSqliteDdlReader_new(SymSqliteDdlReader *this, SymDatabasePlatform *platform) {
    if (this == NULL) {
        this = (SymSqliteDdlReader *) calloc(1, sizeof(SymSqliteDdlReader));
    }
    SymDdlReader *super = SymDdlReader_new(&this->super);
    super->read_table = (void *) &SymSqliteDdlReader_read_table;
    this->platform = platform;
    this->destroy = (void *) &SymSqliteDdlReader_destroy;
    return this;
}
