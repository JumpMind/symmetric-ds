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
#include "db/sql/DmlStatement.h"

static void append_table_name(SymDmlStatement *this, SymStringBuilder *sb) {
    sb->append(sb, SymTable_getFullyQualifiedTableName(this->table->catalog, this->table->schema, this->table->name,
            this->databaseInfo->delimiterToken, this->databaseInfo->catalogSeparator, this->databaseInfo->schemaSeparator));
}

static void append_columns(SymDmlStatement *this, SymStringBuilder *sb) {
    SymIterator *iter = this->table->columns->iterator(this->table->columns);
    while (iter->hasNext(iter)) {
        SymColumn *column = (SymColumn *) iter->next(iter);
        if (iter->index > 0) {
            sb->append(sb, ", ");
        }
        sb->append(sb, this->databaseInfo->delimiterToken);
        sb->append(sb, column->name);
        sb->append(sb, this->databaseInfo->delimiterToken);
    }
    iter->destroy(iter);
}

static void append_column_questions(SymDmlStatement *this, SymStringBuilder *sb) {
    SymIterator *iter = this->table->columns->iterator(this->table->columns);
    while (iter->hasNext(iter)) {
        iter->next(iter);
        if (iter->index > 0) {
            sb->append(sb, ", ");
        }
        sb->append(sb, "?");
    }
    iter->destroy(iter);
}

static void build_insert(SymDmlStatement *this) {
    SymStringBuilder *sb = SymStringBuilder_newWithString("insert into ");
    append_table_name(this, sb);
    sb->append(sb, " (");
    append_columns(this, sb);
    sb->append(sb, ") values (");
    append_column_questions(this, sb);
    sb->append(sb, ")");
    this->sql = sb->destroyAndReturn(sb);
}

static void append_columns_equals_set(SymDmlStatement *this, SymStringBuilder *sb) {
    SymIterator *iter = this->table->columns->iterator(this->table->columns);
    while (iter->hasNext(iter)) {
        SymColumn *column = (SymColumn *) iter->next(iter);
        if (iter->index > 0) {
            sb->append(sb, ", ");
        }
        sb->append(sb, this->databaseInfo->delimiterToken);
        sb->append(sb, column->name);
        sb->append(sb, this->databaseInfo->delimiterToken);
        sb->append(sb, " = ?");
    }
    iter->destroy(iter);
}

static void append_columns_equals(SymDmlStatement *this, SymStringBuilder *sb) {
    SymIterator *iter = this->table->columns->iterator(this->table->columns);
    int count = 0;
    while (iter->hasNext(iter)) {
        SymColumn *column = (SymColumn *) iter->next(iter);
        if (column->isPrimaryKey) {
            if (count++ > 0) {
                sb->append(sb, " and ");
            }
            sb->append(sb, this->databaseInfo->delimiterToken);
            sb->append(sb, column->name);
            sb->append(sb, this->databaseInfo->delimiterToken);
            if (this->nullKeyIndicators != NULL && this->nullKeyIndicators->get(this->nullKeyIndicators, iter->index)) {
                sb->append(sb, " is NULL");
            } else {
                sb->append(sb, " = ?");
            }
        }
    }
    iter->destroy(iter);
}

static void build_update(SymDmlStatement *this) {
    SymStringBuilder *sb = SymStringBuilder_newWithString("update ");
    append_table_name(this, sb);
    sb->append(sb, " set ");
    append_columns_equals_set(this, sb);
    sb->append(sb, " where ");
    append_columns_equals(this, sb);
    this->sql = sb->destroyAndReturn(sb);
}

static void build_delete(SymDmlStatement *this) {
    SymStringBuilder *sb = SymStringBuilder_newWithString("delete from ");
    append_table_name(this, sb);
    sb->append(sb, " where ");
    append_columns_equals(this, sb);
    this->sql = sb->destroyAndReturn(sb);
}

static void build_select(SymDmlStatement *this) {
    SymStringBuilder *sb = SymStringBuilder_newWithString("select ");
    append_columns(this, sb);
    sb->append(sb, " from ");
    append_table_name(this, sb);
    sb->append(sb, " where ");
    append_columns_equals(this, sb);
    this->sql = sb->destroyAndReturn(sb);
}

static void build_sql_types_list(SymList *sqlTypes, SymList *columns, int isPrimaryKey) {
    SymIterator *iter = columns->iterator(columns);
    while (iter->hasNext(iter)) {
        SymColumn *column = (SymColumn *) iter->next(iter);
        if (column->isPrimaryKey == isPrimaryKey) {
            sqlTypes->add(sqlTypes, &column->sqlType);
        }
    }
    iter->destroy(iter);
}

static void build_sql_types(SymDmlStatement *this, SymDmlType dmlType, SymTable *table) {
    this->sqlTypes = SymList_new(NULL);
    if (dmlType == SYM_DML_TYPE_INSERT) {
        build_sql_types_list(this->sqlTypes, table->columns, -1);
    } else if (dmlType == SYM_DML_TYPE_UPDATE) {
        build_sql_types_list(this->sqlTypes, table->columns, 0);
        build_sql_types_list(this->sqlTypes, table->columns, 1);
    } else if (dmlType == SYM_DML_TYPE_DELETE) {
        build_sql_types_list(this->sqlTypes, table->columns, 1);
    }
}

void SymDmlStatement_destroy(SymDmlStatement *this) {
    free(this->sql);
    free(this);
}

SymDmlStatement * SymDmlStatement_new(SymDmlStatement *this, SymDmlType dmlType, SymTable *table, SymList *nullKeyIndicators,
        SymDatabaseInfo *databaseInfo) {
    if (this == NULL) {
        this = (SymDmlStatement *) calloc(1, sizeof(SymDmlStatement));
    }
    this->dmlType = dmlType;
    this->table = table;
    this->databaseInfo = databaseInfo;
    this->nullKeyIndicators = nullKeyIndicators;
    this->destroy = (void *) &SymDmlStatement_destroy;

    build_sql_types(this, dmlType, table);
    if (dmlType == SYM_DML_TYPE_INSERT) {
        build_insert(this);
    } else if (dmlType == SYM_DML_TYPE_UPDATE) {
        build_update(this);
    } else if (dmlType == SYM_DML_TYPE_DELETE) {
        build_delete(this);
    } else if (dmlType == SYM_DML_TYPE_SELECT) {
        build_select(this);
    }

    return this;
}
