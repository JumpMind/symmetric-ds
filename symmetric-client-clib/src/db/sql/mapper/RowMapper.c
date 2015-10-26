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
#include "db/sql/mapper/RowMapper.h"

SymRow * SymRowMapper_mapper(SymRow *row) {
    SymRow *newRow = SymRow_new(NULL, row->map->size);
    SymList *entries = row->map->entries(row->map);
    SymIterator *iter = entries->iterator(entries);
    while (iter->hasNext(iter)) {
        SymMapEntry *entry = (SymMapEntry *) iter->next(iter);
        SymRowEntry *rowEntry = (SymRowEntry *) entry->value;
        char *columnName = strdup(entry->key);
        char *columnValue = strdup(rowEntry->value);
        newRow->put(newRow, columnName, columnValue, rowEntry->sqlType, rowEntry->size);
    }
    iter->destroy(iter);
    entries->destroy(entries);
    return newRow;
}
