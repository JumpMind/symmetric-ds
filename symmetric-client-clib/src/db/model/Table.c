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

char * SymTable_get_full_table_name(SymTable *this, char *delimiterToken, char *catalogSeparator, char *schemaSeparator) {
    SymStringBuilder *sb = SymStringBuilder_new();
    if (this->catalog != NULL) {
        sb->append(sb, delimiterToken)->append(sb, this->catalog)->append(sb, delimiterToken)->append(sb, catalogSeparator);
    }
    if (this->schema != NULL) {
        sb->append(sb, delimiterToken)->append(sb, this->schema)->append(sb, delimiterToken)->append(sb, schemaSeparator);
    }
    sb->append(sb, delimiterToken)->append(sb, this->name)->append(sb, delimiterToken);
    return sb->destroy_and_return(sb);
}

SymColumn * SymTable_find_column(SymTable *this, char *name, unsigned short caseSensitive) {
    SymColumn *column = NULL;
    SymIterator *iter = this->columns->iterator(this->columns);
    while (iter->has_next(iter)) {
        SymColumn *nextColumn = (SymColumn *) iter->next(iter);
        if ((caseSensitive && strcmp(name, nextColumn->name) == 0) || strcasecmp(name, nextColumn->name) == 0) {
            column = nextColumn;
            break;
        }
    }
    iter->destroy(iter);
    return column;
}

SymTable * SymTable_copy_and_filter_columns(SymTable *this, SymTable *source, unsigned short setPrimaryKeys) {
    SymList *orderedColumns = SymList_new(NULL);
    SymIterator *iter = source->columns->iterator(source->columns);
    while (iter->has_next(iter)) {
        SymColumn *srcColumn = (SymColumn *) iter->next(iter);
        SymColumn *column = this->find_column(this, srcColumn->name, 0);
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

void SymTable_copy_column_types_from(SymTable *this, SymTable *source) {
    SymIterator *srcIter = source->columns->iterator(source->columns);
    while (srcIter->has_next(srcIter)) {
        SymColumn *srcColumn = (SymColumn *) srcIter->next(srcIter);
        SymColumn *column = this->find_column(this, srcColumn->name, 0);
        if (column) {
            column->sqlType = srcColumn->sqlType;
        }
    }
    srcIter->destroy(srcIter);
}

char * SymTable_to_string(SymTable *this) {
    SymStringBuilder *sb = SymStringBuilder_new_with_string("Table [name=");
    sb->append(sb, this->name);
    sb->append(sb, "; catalog=")->append(sb, this->catalog);
    sb->append(sb, "; schema=")->append(sb, this->schema);
    sb->append(sb, "] columns:");
    if (this->columns) {
        SymIterator *iter = this->columns->iterator(this->columns);
        while (iter->has_next(iter)) {
            SymColumn *column = (SymColumn *) iter->next(iter);
            char *toString = column->to_string(column);
            sb->append(sb, " ")->append(sb, toString);
            free(toString);
        }
        iter->destroy(iter);
    }
    return sb->destroy_and_return(sb);
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
    this->copy_and_filter_columns = (void *) &SymTable_copy_and_filter_columns;
    this->copy_column_types_from = (void *) &SymTable_copy_column_types_from;
    this->find_column = (void *) &SymTable_find_column;
    this->to_string = (void *) &SymTable_to_string;
    this->destroy = (void *) &SymTable_destroy;
    return this;
}

SymTable * SymTable_new_with_name(SymTable *this, char *name) {
    this = SymTable_new(this);
    this->name = SymStringBuilder_copy(name);
    return this;
}

SymTable * SymTable_new_with_fullname(SymTable *this, char *catalog, char *schema, char *name) {
    this = SymTable_new(this);
    this->catalog = SymStringBuilder_copy(catalog);
    this->schema = SymStringBuilder_copy(schema);
    this->name = SymStringBuilder_copy(name);
    return this;
}
