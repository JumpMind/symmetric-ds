/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.db;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Adds Catalog & Schema aware finder for ddlutils Database class.
 * 
 * Also caches the index of the table to speed up Table lookup.
 * 
 */
public class DatabaseModel extends org.jumpmind.symmetric.ddl.model.Database {

    private static final long serialVersionUID = 1L;
    private Map<String, Integer> tableIndexCache = new HashMap<String, Integer>();

    /**
     * Catalog & Schema aware finder for ddlutils Database class
     * 
     * 
     * @param catalogName
     * @param schemaName
     * @param tableName
     * @param caseSensitive
     * @return
     */
    public Table findTable(String catalogName, String schemaName, String tableName,
            boolean caseSensitive) {
        String cacheKey = catalogName + "." + schemaName + "." + tableName + "." + caseSensitive;
        Integer tableIndex = tableIndexCache.get(cacheKey);
        if (tableIndex != null) {
            if (tableIndex < getTableCount()) {
                Table table = getTable(tableIndex);
                if (doesMatch(table, catalogName, schemaName, tableName, caseSensitive)) {
                    return table;
                }
            }
        }

        Table[] tables = super.getTables();
        for (int i = 0; i < tables.length; i++) {
            Table table = tables[i];
            if (doesMatch(table, catalogName, schemaName, tableName, caseSensitive)) {
                tableIndexCache.put(cacheKey, i);
                return table;
            }
        }
        return null;
    }

    private boolean doesMatch(Table table, String catalogName, String schemaName, String tableName,
            boolean caseSensitive) {
        if (caseSensitive) {
            return ((catalogName == null || (catalogName != null && catalogName.equals(table
                    .getCatalog())))
                    && (schemaName == null || (schemaName != null && schemaName.equals(table
                            .getSchema()))) && table.getName().equals(tableName));
        } else {
            return ((catalogName == null || (catalogName != null && catalogName
                    .equalsIgnoreCase(table.getCatalog())))
                    && (schemaName == null || (schemaName != null && schemaName
                            .equalsIgnoreCase(table.getSchema()))) && table.getName()
                    .equalsIgnoreCase(tableName));
        }
    }

    public Table findTable(String catalogName, String schemaName, String tableName) {
        return findTable(catalogName, schemaName, tableName, false);
    }

    public void resetTableIndexCache() {
        tableIndexCache.clear();
    }
}