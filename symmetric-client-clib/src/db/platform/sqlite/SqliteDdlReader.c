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

static int SymSqliteDdlReader_toSqlType(char *type) {
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
    } else if (strncasecmp(type, "BOOLEAN", 7) == 0) {
        sqlType = SYM_SQL_TYPE_BOOLEAN;
    } else if (strncasecmp(type, "BIT", 3) == 0) {
        sqlType = SYM_SQL_TYPE_BIT;
    } else {
        sqlType = SYM_SQL_TYPE_VARCHAR;
    }
    return sqlType;
}

static void * SymSqliteDdlReader_columnMapper(SymRow *row) {
    char *name = row->getStringNew(row, "name");
    int isPrimaryKey = row->getInt(row, "pk");
    SymColumn *column = SymColumn_new(NULL, name, isPrimaryKey);
    free(name);
    column->isRequired = row->getInt(row, "notnull");
    column->sqlType = SymSqliteDdlReader_toSqlType(row->getString(row, "type"));
    return column;
}

SymTable * SymSqliteDdlReader_readTable(SymSqliteDdlReader *this, char *catalog, char *schema, char *tableName) {
    SymTable *table = NULL;
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sql = SymStringBuilder_newWithString("pragma table_info(");
    sql->append(sql, tableName)->append(sql, ")");
    int error;
    SymList *columns = sqlTemplate->query(sqlTemplate, sql->str, NULL, NULL, &error, SymSqliteDdlReader_columnMapper);
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
    super->readTable = (void *) &SymSqliteDdlReader_readTable;
    this->platform = platform;
    this->destroy = (void *) &SymSqliteDdlReader_destroy;
    return this;
}
