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
#ifndef SYM_SQL_TEMPLATE_H
#define SYM_SQL_TEMPLATE_H

#include <stdio.h>
#include <stdlib.h>
#include "db/sql/SqlTransaction.h"
#include "db/sql/Row.h"
#include "util/List.h"
#include "util/StringArray.h"

typedef enum {
    SYM_SQL_TYPE_BIT, SYM_SQL_TYPE_TINYINT, SYM_SQL_TYPE_SMALLINT, SYM_SQL_TYPE_INTEGER, SYM_SQL_TYPE_BIGINT,
    SYM_SQL_TYPE_FLOAT, SYM_SQL_TYPE_REAL, SYM_SQL_TYPE_DOUBLE, SYM_SQL_TYPE_NUMERIC, SYM_SQL_TYPE_DECIMAL,
    SYM_SQL_TYPE_CHAR, SYM_SQL_TYPE_VARCHAR, SYM_SQL_TYPE_LONGVARCHAR,
    SYM_SQL_TYPE_DATE, SYM_SQL_TYPE_TIME, SYM_SQL_TYPE_TIMESTAMP,
    SYM_SQL_TYPE_BINARY, SYM_SQL_TYPE_VARBINARY, SYM_SQL_TYPE_LONGVARBINARY,
    SYM_SQL_TYPE_NULL,
    SYM_SQL_TYPE_OTHER,
    SYM_SQL_TYPE_JAVA_OBJECT,
    SYM_SQL_TYPE_DISTINCT,
    SYM_SQL_TYPE_STRUCT, SYM_SQL_TYPE_ARRAY,
    SYM_SQL_TYPE_BLOB, SYM_SQL_TYPE_CLOB,
    SYM_SQL_TYPE_REF, SYM_SQL_TYPE_DATALINK,
    SYM_SQL_TYPE_BOOLEAN,
    SYM_SQL_TYPE_ROWID,
    SYM_SQL_TYPE_NCHAR, SYM_SQL_TYPE_NVARCHAR, SYM_SQL_TYPE_LONGNVARCHAR, SYM_SQL_TYPE_NCLOB,
    SYM_SQL_TYPE_SQLXML
} SymSqlType;

typedef struct SymSqlTemplate {
    int (*queryForInt)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    long (*queryForLong)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    char * (*queryForString)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    SymList * (*queryForList)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    void * (*queryForObject)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error, void *map_row(SymRow *row));
    SymList * (*query)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error, void *map_row(SymRow *row));
    SymList * (*queryWithUserData)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error,
            void *map_row(SymRow *row, void *userData), void *userData);
    int (*update)(struct SymSqlTemplate *this, char *sql, SymStringArray *args, SymList *sqlTypes, int *error);
    SymSqlTransaction * (*startSqlTransaction)(struct SymSqlTemplate *this);
    void (*destroy)(struct SymSqlTemplate *this);
} SymSqlTemplate;

SymSqlTemplate * SymSqlTemplate_new(SymSqlTemplate *this);

#endif
