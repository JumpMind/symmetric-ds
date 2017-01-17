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
#include "db/model/Table.h"

char * SymTable_getFullyQualifiedTableName(char *catalogName, char *schemaName,
        char *tableName, char *quoteString, char *catalogSeparator, char *schemaSeparator) {
    if (quoteString == NULL) {
        quoteString = "";
    }
    char *prefix = SymTable_getFullyQualifiedTablePrefix(catalogName, schemaName, quoteString, catalogSeparator, schemaSeparator);
    SymStringBuilder *sb = SymStringBuilder_newWithString(prefix);
    free(prefix);
    sb->append(sb, quoteString)->append(sb, tableName)->append(sb, quoteString);
    return sb->destroyAndReturn(sb);
}

char * SymTable_getFullyQualifiedTablePrefix(char *catalogName, char *schemaName,
        char *quoteString, char *catalogSeparator, char *schemaSeparator) {
    if (quoteString == NULL) {
        quoteString = "";
    }
    SymStringBuilder *sb = SymStringBuilder_new();
    if (SymStringUtils_isNotBlank(catalogName)) {
        sb->append(sb, quoteString)->append(sb, catalogName)->append(sb, quoteString)->append(sb, catalogSeparator);
    }
    if (SymStringUtils_isNotBlank(schemaName)) {
        sb->append(sb, quoteString)->append(sb, schemaName)->append(sb, quoteString)->append(sb, schemaSeparator);
    }
    return sb->destroyAndReturn(sb);
}

static int SymTable_calculateHashcodeForColumns(int prime, SymList *cols) {
    int result = 1;
    SymIterator *iter = cols->iterator(cols);
    while (iter->hasNext(iter)) {
        SymColumn *col = (SymColumn *) iter->next(iter);
        result = prime * result + SymStringBuilder_hashCode(col->name);
        result = prime * result + col->sqlType;
    }
    iter->destroy(iter);
    return result;
}

char * SymTable_getCommaDeliminatedColumns(SymList *cols) {
    if (cols != NULL && cols->size > 0) {
        SymStringBuilder *columns = SymStringBuilder_new(NULL);
        int i;
        for (i = 0; i < cols->size; i++) {
            SymColumn *column = cols->get(cols, i);
            columns->append(columns, column->name);
            if (i < (cols->size-1)) {
                columns->append(columns, ",");
            }
        }
        return columns->destroyAndReturn(columns);
    }
    else {
        return SymStringUtils_format("%s", " ");
    }
}

int SymTable_calculateTableHashcode(SymTable *this) {
    int prime = 31;
    int result = 1;
    result = prime * result + SymStringBuilder_hashCode(this->name);
    result = prime * result + SymTable_calculateHashcodeForColumns(prime, this->columns);
    return result;
}

char * SymTable_getFullyQualifiedTableNameThis(SymTable *this) {
    return SymTable_getFullyQualifiedTableName(this->catalog, this->schema, this->name, "", ".", ".");
}

char * SymTable_getTableKey(SymTable *this) {
    char *name = this->getFullyQualifiedTableName(this);
    SymStringBuilder *sb = SymStringBuilder_newWithString(name);
    sb->append(sb, "-")->appendf(sb, "%d", SymTable_calculateTableHashcode(this));
    free(name);
    return sb->destroyAndReturn(sb);
}

SymColumn * SymTable_findColumn(SymTable *this, char *name, unsigned short caseSensitive) {
    SymColumn *column = NULL;
    SymIterator *iter = this->columns->iterator(this->columns);
    while (iter->hasNext(iter)) {
        SymColumn *nextColumn = (SymColumn *) iter->next(iter);
        if ((caseSensitive && strcmp(name, nextColumn->name) == 0) || strcasecmp(name, nextColumn->name) == 0) {
            column = nextColumn;
            break;
        }
    }
    iter->destroy(iter);
    return column;
}

SymTable * SymTable_copyAndFilterColumns(SymTable *this, SymList *sourceColumns, unsigned short setPrimaryKeys) {
    SymList *orderedColumns = SymList_new(NULL);
    SymIterator *iter = sourceColumns->iterator(sourceColumns);
    while (iter->hasNext(iter)) {
        SymColumn *srcColumn = (SymColumn *) iter->next(iter);
        SymColumn *column = this->findColumn(this, srcColumn->name, 0);
        if (column) {
            // TODO: clone the SymColumn?
            orderedColumns->add(orderedColumns, column);
            if (setPrimaryKeys) {
                column->isPrimaryKey = srcColumn->isPrimaryKey;
            }
        }
    }
    iter->destroy(iter);

    SymTable *copy = SymTable_new(NULL);
    copy->catalog = SymStringBuilder_copy(this->catalog);
    copy->schema = SymStringBuilder_copy(this->schema);
    copy->name = SymStringBuilder_copy(this->name);
    copy->columns = orderedColumns;
    return copy;
}

void SymTable_copyColumnTypesFrom(SymTable *this, SymTable *source) {
    SymIterator *srcIter = source->columns->iterator(source->columns);
    while (srcIter->hasNext(srcIter)) {
        SymColumn *srcColumn = (SymColumn *) srcIter->next(srcIter);
        SymColumn *column = this->findColumn(this, srcColumn->name, 0);
        if (column) {
            column->sqlType = srcColumn->sqlType;
        }
    }
    srcIter->destroy(srcIter);
}

char * SymTable_toString(SymTable *this) {
    SymStringBuilder *sb = SymStringBuilder_newWithString("Table [name=");
    sb->append(sb, this->name);
    sb->append(sb, "; catalog=")->append(sb, this->catalog);
    sb->append(sb, "; schema=")->append(sb, this->schema);
    sb->append(sb, "] columns:");
    if (this->columns) {
        SymIterator *iter = this->columns->iterator(this->columns);
        while (iter->hasNext(iter)) {
            SymColumn *column = (SymColumn *) iter->next(iter);
            char *toString = column->toString(column);
            sb->append(sb, " ")->append(sb, toString);
            free(toString);
        }
        iter->destroy(iter);
    }
    return sb->destroyAndReturn(sb);
}

SymList * SymTable_getPrimaryKeyColumns(SymTable *this) {
    SymList *primaryKeyColumns = SymList_new(NULL);
    int i;
    for (i = 0; i < this->columns->size; ++i) {
        SymColumn *column = this->columns->get(this->columns, i);
        if (column->isPrimaryKey) {
            primaryKeyColumns->add(primaryKeyColumns, column);
        }
    }
    return primaryKeyColumns;
}

SymStringArray * SymTable_getColumnNames(SymTable *this) {
    SymStringArray * columnNames = SymStringArray_new(NULL);
    int i;
    for (i = 0; i < this->columns->size; ++i) {
        SymColumn *column = this->columns->get(this->columns, i);
        columnNames->add(columnNames, column->name);
    }
    return columnNames;
}

void SymTable_destroy(SymTable *this) {
//    free(this->name); probably stack memory.
//    free(this->catalog);
//    free(this->schema);
    this->columns->destroyAll(this->columns, (void *)SymColumn_destroy);
    free(this);
}

SymTable * SymTable_new(SymTable *this) {
    if (this == NULL) {
        this = (SymTable *) calloc(1, sizeof(SymTable));
    }
    this->copyAndFilterColumns = (void *) &SymTable_copyAndFilterColumns;
    this->copyColumnTypesFrom = (void *) &SymTable_copyColumnTypesFrom;
    this->findColumn = (void *) &SymTable_findColumn;
    this->toString = (void *) &SymTable_toString;
    this->calculateTableHashcode = (void *) &SymTable_calculateTableHashcode;
    this->getTableKey = (void *) &SymTable_getTableKey;
    this->getFullyQualifiedTableName = (void *) &SymTable_getFullyQualifiedTableNameThis;
    this->getPrimaryKeyColumns = (void *) &SymTable_getPrimaryKeyColumns;
    this->getColumnNames = (void *) &SymTable_getColumnNames;
    this->destroy = (void *) &SymTable_destroy;
    return this;
}

SymTable * SymTable_newWithName(SymTable *this, char *name) {
    this = SymTable_new(this);
    this->name = SymStringBuilder_copy(name);
    return this;
}

SymTable * SymTable_newWithFullname(SymTable *this, char *catalog, char *schema, char *name) {
    this = SymTable_new(this);
    this->catalog = SymStringBuilder_copy(catalog);
    this->schema = SymStringBuilder_copy(schema);
    this->name = SymStringBuilder_copy(name);
    return this;
}
