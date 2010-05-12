package org.jumpmind.symmetric.db;

import java.util.HashMap;
import java.util.Map;

import org.apache.ddlutils.model.Table;

/**
 * Adds Catalog & Schema aware finder for ddlutils Database class.
 * 
 * also caches the index of the table to speed up Table lookup.
 * 
 * 
 * @author Lari Hotari
 * 
 */
public class DatabaseModel extends org.apache.ddlutils.model.Database {

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
