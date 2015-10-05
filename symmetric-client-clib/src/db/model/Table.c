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

char * SymTable_getFullTableName(SymTable *this, char *delimiterToken, char *catalogSeparator, char *schemaSeparator) {
    SymStringBuilder *sb = SymStringBuilder_new();
    if (this->catalog != NULL) {
        sb->append(sb, delimiterToken)->append(sb, this->catalog)->append(sb, delimiterToken)->append(sb, catalogSeparator);
    }
    if (this->schema != NULL) {
        sb->append(sb, delimiterToken)->append(sb, this->schema)->append(sb, delimiterToken)->append(sb, schemaSeparator);
    }
    sb->append(sb, delimiterToken)->append(sb, this->name)->append(sb, delimiterToken);
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

SymTable * SymTable_copyAndFilterColumns(SymTable *this, SymTable *source, unsigned short setPrimaryKeys) {
    SymList *orderedColumns = SymList_new(NULL);
    SymIterator *iter = source->columns->iterator(source->columns);
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

void SymTable_destroy(SymTable *this) {
    free(this->name);
    free(this->catalog);
    free(this->schema);
    this->columns->destroy(this->columns);
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
