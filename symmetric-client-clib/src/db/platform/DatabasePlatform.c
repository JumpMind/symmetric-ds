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
#include "db/platform/DatabasePlatform.h"

SymTable * SymDatabasePlatform_getTableFromCache(SymDatabasePlatform *this, char *catalog, char *schema, char *tableName, unsigned int forceReread) {
    // TODO: need a hash table and caching of Table objects
    return this->readTableFromDatabase(this, catalog, schema, tableName);
}

SymTable * SymDatabasePlatform_readTableFromDatabase(SymDatabasePlatform *this, char *catalog, char *schema, char *tableName) {
    if (catalog == NULL) {
        catalog = this->defaultCatalog;
    }
    if (schema == NULL) {
        schema = this->defaultSchema;
    }
    SymTable *table = this->ddlReader->readTable(this->ddlReader, catalog, schema, tableName);
    return table;
}

void SymDatabasePlatform_resetCachedTableModel(SymDatabasePlatform *this) {
	// TODO: implement along with caching.
}

SymTable * SymDatabasePlatform_makeAllColumnsPrimaryKeys(SymDatabasePlatform *this, SymTable *table) {
    // TODO Should clone table.
    SymTable *result = table;
    int i;
    for (i = 0; i < result->columns->size; ++i) {
        SymColumn *column = result->columns->get(result->columns, i);
        // TODO check for LOB
        column->isPrimaryKey = 1;
    }

    return result;
}


void SymDatabasePlatform_destroy(SymDatabasePlatform *this) {
}

SymDatabasePlatform * SymDatabasePlatform_new(SymDatabasePlatform *this) {
    if (this != NULL) {
        this->getTableFromCache = (void *) &SymDatabasePlatform_getTableFromCache;
        this->readTableFromDatabase = (void *) &SymDatabasePlatform_readTableFromDatabase;
        this->resetCachedTableModel = (void *)&SymDatabasePlatform_resetCachedTableModel;
        this->makeAllColumnsPrimaryKeys = (void *)&SymDatabasePlatform_makeAllColumnsPrimaryKeys;
        this->destroy = (void *) &SymDatabasePlatform_destroy;
    }
    return this;
}
