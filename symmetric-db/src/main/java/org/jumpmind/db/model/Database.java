package org.jumpmind.db.model;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.Serializable;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents the database model, ie. the tables in the database. It also
 * contains the corresponding dyna classes for creating dyna beans for the
 * objects stored in the tables.
 */
public class Database implements Serializable, Cloneable {

    protected static final Logger log = LoggerFactory.getLogger(Database.class);

    /** Unique ID for serialization purposes. */
    private static final long serialVersionUID = -3160443396757573868L;

    /** The name of the database model. */
    private String name;

    private String catalog;
    
    private String schema;
    
    /** The method for generating primary keys (currently ignored). */
    private String idMethod;

    /** The version of the model. */
    private String version;

    /** The tables. */
    private ArrayList<Table> tables = new ArrayList<Table>();

    private Map<String, Integer> tableIndexCache = new HashMap<String, Integer>();

    /**
     * Implements modified topological sort of tables (@see <a
     * href="http://en.wikipedia.org/wiki/Topological_sorting">topological
     * sorting</a>). The 'depth-first search' is implemented in order to detect
     * and ignore cycles.
     * 
     * @param tables
     *          List of tables to sort.
     * @param allTables
     *          List of tables in database, if null the tables param will be used.
     * @param tablePrefix
     *          The SymmetricDS runtime table prefix.
     * @param dependencyMap
     *          Map to separate dependent tables into groups.  The key will be an integer based counter (1,2...) to identify the grouping.  
     *          The value will contain all the tables that are dependent on each other but independent for other tables in other groups.
     *          Used to identify which tables could be placed in a specific group.
     *          This should be passed in empty so that it can be used by reference after the method finishes.
     * @param missingDependencyMap
     *          This is a used for any tables that are missing from the tables param that should be included in synchronization to avoid FK issues.                    
     * @return List of tables in their dependency order - if table A has a
     *         foreign key for table B then table B will precede table A in the
     *         list.
     */
    public static List<Table> sortByForeignKeys(List<Table> tables, Map<String, Table> allTables,
            Map<Integer, Set<Table>> dependencyMap, Map<Table, Set<String>> missingDependencyMap) {
        
        if (allTables == null) {
            allTables = new HashMap<String, Table>();
            for (Table t : tables) {
                allTables.put(t.getName(), t);
            }
        }
        
        if (dependencyMap == null) {
            dependencyMap = new HashMap<Integer, Set<Table>>();
        }
        if (missingDependencyMap == null) {
            missingDependencyMap = new HashMap<Table, Set<String>>();
        }
        Set<Table> resolved = new HashSet<Table>();
        Set<Table> temporary = new HashSet<Table>();
        List<Table> finalList = new ArrayList<Table>();
        
        MutableInt depth = new MutableInt(1);
        MutableInt position = new MutableInt(1);
        MutableInt parentPosition = new MutableInt(-1);
        
        Map<Table, Integer> resolvedPosition = new HashMap<Table, Integer>();
        
        for(Table t : tables) {
            if (t != null) {
                depth.setValue(1);
                parentPosition.setValue(-1);
                resolveForeignKeyOrder(t, allTables, resolved, temporary, finalList, null, missingDependencyMap, 
                        dependencyMap, depth, position, resolvedPosition, parentPosition);
            }
        }
    
        Collections.reverse(finalList);
        return finalList;
    }

    public static void logMissingDependentTableNames(List<Table> tables) {
        Map<String, List<String>> missingTablesByChildTable = findMissingDependentTableNames(tables);
        for (String childTableName : missingTablesByChildTable.keySet()) {
            List<String> missingTables = missingTablesByChildTable.get(childTableName);
            StringBuilder dependentTables = new StringBuilder();
            for (String missingTableName : missingTables) {
                if (dependentTables.length() > 0) {
                    dependentTables.append(", ");
                }
                dependentTables.append(missingTableName);
            }
            log.info("Unable to resolve foreign keys for table " + childTableName + " because the following dependent tables were not included [" + dependentTables.toString() + "].");
        }
    }
    
    public static Map<String, List<String>> findMissingDependentTableNames(List<Table> tables) {
        Map<String, List<String>> missingTablesByChildTable = new HashMap<String, List<String>>();
        Map<String, Table> allTables = new HashMap<String, Table>();
        for (Table t : tables) {
            allTables.put(t.getName(), t);
        }

        for (Table table : tables) {
            List<String> missingTables = missingTablesByChildTable.get(table.getName());
            for (ForeignKey fk : table.getForeignKeys()) {
                if (allTables.get(fk.getForeignTableName()) == null) {
                    if (missingTables == null) {
                        missingTables = new ArrayList<String>();
                        missingTablesByChildTable.put(table.getName(), missingTables);
                    }
                    missingTables.add(fk.getForeignTableName());
                }
            }
        }
        return missingTablesByChildTable;
    }

    public static void resolveForeignKeyOrder(Table t, Map<String, Table> allTables, Set<Table> resolved, Set<Table> temporary, 
            List<Table> finalList, Table parentTable, Map<Table, Set<String>> missingDependencyMap,
            Map<Integer, Set<Table>> dependencyMap, MutableInt depth, MutableInt position, 
            Map<Table, Integer> resolvedPosition, MutableInt parentPosition) {
        
        if (resolved.contains(t)) { 
            parentPosition.setValue(resolvedPosition.get(t));
            return; 
        }

        if (!temporary.contains(t) && !resolved.contains(t)) {
            Set<Integer> parentTablesChannels = new HashSet<Integer>();
            if (t == null) {
                if (parentTable != null) {
                    for (ForeignKey fk : parentTable.getForeignKeys()) {
                        if (allTables.get(fk.getForeignTableName()) == null) {
                            if (missingDependencyMap.get(parentTable) == null) {
                                missingDependencyMap.put(parentTable, new HashSet<String>());
                            }
                            missingDependencyMap.get(parentTable).add(fk.getForeignTableName());
                        }
                    }
                }
            } else {
                temporary.add(t);
                
                for (ForeignKey fk : t.getForeignKeys()) {
                    Table fkTable = allTables.get(fk.getForeignTableName());
                    if (fkTable != t) {
                        depth.increment(); 
                        resolveForeignKeyOrder(fkTable, allTables, resolved, temporary, finalList, t, missingDependencyMap, 
                                dependencyMap, depth, position, resolvedPosition, parentPosition);
                        Integer resolvedParentTableChannel = resolvedPosition.get(fkTable);
                        if (resolvedParentTableChannel != null) {
                            parentTablesChannels.add(resolvedParentTableChannel);
                        }
                        
                   }
                 }
            }
                
            
            if (t != null) {
                if (parentPosition.intValue() > 0) {
                    if (dependencyMap.get(parentPosition.intValue()) == null) { 
                        dependencyMap.put(parentPosition.intValue(), new HashSet<Table>());
                    }
                    
                    if (parentTablesChannels.size() > 1) {
                        parentPosition.setValue(mergeChannels(parentTablesChannels, dependencyMap, resolvedPosition));
                    } 
                    dependencyMap.get(parentPosition.intValue()).add(t);
                }
                else {
                    if (dependencyMap.get(position.intValue()) == null) {
                        dependencyMap.put(position.intValue(), new HashSet<Table>());
                    }
                    
                    dependencyMap.get(position.intValue()).add(t);
                }
                
                resolved.add(t);
                resolvedPosition.put(t, parentPosition.intValue() > 0 ? parentPosition.intValue() : position.intValue());
                finalList.add(0, t);
                
                if (depth.intValue() == 1) {
                    if (parentPosition.intValue() < 0) {
                        position.increment();
                    }
                }
                else {
                    depth.decrement();
                    
                }
            }
        }
    }
    
    protected static Integer mergeChannels(Set<Integer> parentTablesChannels, Map<Integer, Set<Table>> dependencyMap, 
            Map<Table, Integer> resolvedPosition) {
        
        Iterator<Integer> i = parentTablesChannels.iterator();
        Set<Table> mergedTables = new HashSet<Table>();
        Integer minChannelId = null;
        Set<Integer> unusedChannels = new HashSet<Integer>();
        while (i.hasNext()) {
            Integer channelToMerge = (Integer) i.next();
            if (dependencyMap.get(channelToMerge) != null) {
                mergedTables.addAll(dependencyMap.get(channelToMerge));
                
                if (minChannelId == null) { 
                    minChannelId = channelToMerge;
                } else if (channelToMerge < minChannelId) {
                    unusedChannels.add(minChannelId);
                    minChannelId = channelToMerge;
                } else {
                    unusedChannels.add(channelToMerge);
                }
            }
        }
        dependencyMap.put(minChannelId, mergedTables);
        for (Table t : mergedTables) {
            resolvedPosition.put(t, minChannelId);
        }
        for (Integer unusedChannel : unusedChannels) {
            dependencyMap.remove(unusedChannel);
        }
        return minChannelId;
    }
    
    public static String printTables(List<Table> tables) {
        StringBuffer sb = new StringBuffer();
        for (Table t : tables) {
            sb.append(t.getName() + ",");
        }
        return sb.toString();
    }
    
    public static Table[] sortByForeignKeys(Table... tables) {
        if (tables != null) {
            List<Table> list = new ArrayList<Table>(tables.length);
            for (Table table : tables) {
                list.add(table);
            }
            list = sortByForeignKeys(list, null, null, null);
            tables = list.toArray(new Table[list.size()]);
        }
        return tables;
    }

    public static List<Table> sortByForeignKeys(List<Table> tables) {
        return sortByForeignKeys(tables, null, null, null);
    }
    
    /**
     * Adds all tables from the other database to this database. Note that the
     * other database is not changed.
     * 
     * @param otherDb
     *            The other database model
     */
    public void mergeWith(Database otherDb) throws ModelException {
        for (Iterator<Table> it = otherDb.tables.iterator(); it.hasNext();) {
            Table table = (Table) it.next();

            if (findTable(table.getName()) != null) {
                // TODO: It might make more sense to log a warning and overwrite
                // the table (or merge them) ?
                throw new ModelException("Cannot merge the models because table " + table.getName()
                        + " already defined in this model");
            }
            try {
                addTable((Table) table.clone());
            } catch (CloneNotSupportedException ex) {
                // won't happen
            }
        }
    }

    /**
     * Returns the name of this database model.
     * 
     * @return The name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this database model.
     * 
     * @param name
     *            The name
     */
    public void setName(String name) {
        this.name = name;
    }

    public String getCatalog() {
        return catalog;
    }
    
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }
    
    public String getSchema() {
        return schema;
    }
    
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Returns the version of this database model.
     * 
     * @return The version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the version of this database model.
     * 
     * @param version
     *            The version
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Returns the method for generating primary key values.
     * 
     * @return The method
     */
    public String getIdMethod() {
        return idMethod;
    }

    /**
     * Sets the method for generating primary key values. Note that this value
     * is ignored by DdlUtils and only for compatibility with Torque.
     * 
     * @param idMethod
     *            The method
     */
    public void setIdMethod(String idMethod) {
        this.idMethod = idMethod;
    }

    /**
     * Returns the number of tables in this model.
     * 
     * @return The number of tables
     */
    public int getTableCount() {
        return tables.size();
    }

    /**
     * Returns the tables in this model.
     * 
     * @return The tables
     */
    public Table[] getTables() {
        return (Table[]) tables.toArray(new Table[tables.size()]);
    }

    /**
     * Returns the table at the specified position.
     * 
     * @param idx
     *            The index of the table
     * @return The table
     */
    public Table getTable(int idx) {
        return (Table) tables.get(idx);
    }

    /**
     * Adds a table.
     * 
     * @param table
     *            The table to add
     */
    public void addTable(Table table) {
        if (table != null) {
            tables.add(table);
        }
    }

    /**
     * Adds a table at the specified position.
     * 
     * @param idx
     *            The index where to insert the table
     * @param table
     *            The table to add
     */
    public void addTable(int idx, Table table) {
        if (table != null) {
            tables.add(idx, table);
        }
    }

    /**
     * Adds the given tables.
     * 
     * @param tables
     *            The tables to add
     */
    public void addTables(Collection<Table> tables) {
        for (Iterator<Table> it = tables.iterator(); it.hasNext();) {
            addTable((Table) it.next());
        }
    }

    public void addTables(Table[] tables) {
        for (Table table : tables) {
            addTable(table);
        }
    }

    /**
     * Removes the given table.
     * 
     * @param table
     *            The table to remove
     */
    public void removeTable(Table table) {
        if (table != null) {
            tables.remove(table);
        }
    }

    /**
     * Removes the indicated table.
     * 
     * @param idx
     *            The index of the table to remove
     */
    public void removeTable(int idx) {
        tables.remove(idx);
    }

    // Helper methods

    /**
     * Initializes the model by establishing the relationships between elements
     * in this model encoded eg. in foreign keys etc. Also checks that the model
     * elements are valid (table and columns have a name, foreign keys rference
     * existing tables etc.)
     */
    public void initialize() throws ModelException {
        // we have to setup
        // * target tables in foreign keys
        // * columns in foreign key references
        // * columns in indices
        // * columns in uniques
        HashSet<String> namesOfProcessedTables = new HashSet<String>();
        HashSet<String> namesOfProcessedColumns = new HashSet<String>();
        HashSet<String> namesOfProcessedFks = new HashSet<String>();
        HashSet<String> namesOfProcessedIndices = new HashSet<String>();
        int tableIdx = 0;

        for (Iterator<Table> tableIt = tables.iterator(); tableIt.hasNext(); tableIdx++) {
            Table curTable = tableIt.next();

            if ((curTable.getName() == null) || (curTable.getName().length() == 0)) {
                throw new ModelException("The table nr. " + tableIdx + " has no name");
            }
            if (namesOfProcessedTables.contains(curTable.getFullyQualifiedTableName())) {
                throw new ModelException("There are multiple tables with the name "
                        + curTable.getName());
            }
            namesOfProcessedTables.add(curTable.getFullyQualifiedTableName());

            namesOfProcessedColumns.clear();
            namesOfProcessedFks.clear();
            namesOfProcessedIndices.clear();

            for (int idx = 0; idx < curTable.getColumnCount(); idx++) {
                Column column = curTable.getColumn(idx);

                if ((column.getName() == null) || (column.getName().length() == 0)) {
                    throw new ModelException("The column nr. " + idx + " in table "
                            + curTable.getName() + " has no name");
                }
                if (namesOfProcessedColumns.contains(column.getName())) {
                    throw new ModelException("There are multiple column with the name "
                            + column.getName() + " in the table " + curTable.getName());
                }
                namesOfProcessedColumns.add(column.getName());

                if ((column.getMappedType() == null) || (column.getMappedType().length() == 0)) {
                    throw new ModelException("The column nr. " + idx + " in table "
                            + curTable.getName() + " has no type");
                }
                if ((column.getMappedTypeCode() == Types.OTHER)
                        && !"OTHER".equalsIgnoreCase(column.getMappedType())) {
                    throw new ModelException("The column nr. " + idx + " in table "
                            + curTable.getName() + " has an unknown type " + column.getMappedType());
                }
                namesOfProcessedColumns.add(column.getName());
            }

            for (int idx = 0; idx < curTable.getForeignKeyCount(); idx++) {
                ForeignKey fk = curTable.getForeignKey(idx);
                String fkName = (fk.getName() == null ? "" : fk.getName());
                String fkDesc = (fkName.length() == 0 ? "nr. " + idx : fkName);

                if (fkName.length() > 0) {
                    if (namesOfProcessedFks.contains(fkName)) {
                        throw new ModelException("There are multiple foreign keys in table "
                                + curTable.getName() + " with the name " + fkName);
                    }
                    namesOfProcessedFks.add(fkName);
                }

                if (fk.getForeignTable() == null) {
                    Table targetTable = findTable(fk.getForeignTableName(), true);

                    if (targetTable != null) {
                        fk.setForeignTable(targetTable);
                        fk.setForeignTableCatalog(targetTable.getCatalog());
                        fk.setForeignTableSchema(targetTable.getSchema());
                    } else {
                        log.debug("The foreignkey "
                                + fkDesc
                                + " in table "
                                + curTable.getName()
                                + " references the undefined table "
                                + fk.getForeignTableName()
                                + ".  This could be because the foreign key table was in another schema which is a bug that should be fixed in the future.");
                    }
                }
                if (fk.getForeignTable() != null) {
                    for (int refIdx = 0; refIdx < fk.getReferenceCount(); refIdx++) {
                        Reference ref = fk.getReference(refIdx);

                        if (ref.getLocalColumn() == null) {
                            Column localColumn = curTable
                                    .findColumn(ref.getLocalColumnName(), true);

                            if (localColumn == null) {
                                throw new ModelException("The foreignkey " + fkDesc + " in table "
                                        + curTable.getName()
                                        + " references the undefined local column "
                                        + ref.getLocalColumnName());
                            } else {
                                ref.setLocalColumn(localColumn);
                            }
                        }
                        if (ref.getForeignColumn() == null) {
                            Column foreignColumn = fk.getForeignTable().findColumn(
                                    ref.getForeignColumnName(), true);

                            if (foreignColumn == null) {
                                throw new ModelException("The foreignkey " + fkDesc + " in table "
                                        + curTable.getName()
                                        + " references the undefined local column "
                                        + ref.getForeignColumnName() + " in table "
                                        + fk.getForeignTable().getName());
                            } else {
                                ref.setForeignColumn(foreignColumn);
                            }
                        }
                    }
                }
            }

            for (int idx = 0; idx < curTable.getIndexCount(); idx++) {
                IIndex index = curTable.getIndex(idx);
                String indexName = (index.getName() == null ? "" : index.getName());

                if (indexName.length() > 0) {
                    if (namesOfProcessedIndices.contains(indexName)) {
                        throw new ModelException("There are multiple indices in table "
                                + curTable.getName() + " with the name " + indexName);
                    }
                    namesOfProcessedIndices.add(indexName);
                }

                for (int indexColumnIdx = 0; indexColumnIdx < index.getColumnCount(); indexColumnIdx++) {
                    IndexColumn indexColumn = index.getColumn(indexColumnIdx);
                    Column column = curTable.findColumn(indexColumn.getName(), true);
                    indexColumn.setColumn(column);
                }
            }
        }
    }

    /**
     * Finds the table with the specified name, using case insensitive matching.
     * Note that this method is not called getTable to avoid introspection
     * problems.
     * 
     * @param name
     *            The name of the table to find
     * @return The table or <code>null</code> if there is no such table
     */
    public Table findTable(String name) {
        return findTable(name, false);
    }

    /**
     * Finds the table with the specified name, using case insensitive matching.
     * Note that this method is not called getTable) to avoid introspection
     * problems.
     * 
     * @param name
     *            The name of the table to find
     * @param caseSensitive
     *            Whether case matters for the names
     * @return The table or <code>null</code> if there is no such table
     */
    public Table findTable(String name, boolean caseSensitive) {
        for (Iterator<Table> iter = tables.iterator(); iter.hasNext();) {
            Table table = (Table) iter.next();

            if (caseSensitive) {
                if (table.getName().equals(name)) {
                    return table;
                }
            } else {
                if (table.getName().equalsIgnoreCase(name)) {
                    return table;
                }
            }
        }
        return null;
    }

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

        Table[] tables = getTables();
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
    
    public void removeAllTablesExcept(String... tableNames) {
        Iterator<Table> tableIterator = this.tables.iterator();
        while(tableIterator.hasNext()) {
            Table table = tableIterator.next();
            boolean foundTable = false;
            for (String tableName : tableNames) {
                if (tableName.equals(table.getName())) {
                    foundTable = true;
                    break;
                }
            }
            
            if (!foundTable) {
                tableIterator.remove();
            }
        }
    }
    
    public Database copy() {
        try {
            return (Database) this.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException {
        Database result = (Database) super.clone();
        result.name = name;
        result.catalog = catalog;
        result.schema = schema;
        result.idMethod = idMethod;
        result.version = version;
        result.tables = new ArrayList<Table>(tables.size());
        for (Table table : tables) {
            result.tables.add((Table)table.clone());
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {
        if (obj instanceof Database) {
            Database other = (Database) obj;

            // Note that this compares case sensitive
            return new EqualsBuilder().append(name, other.name).append(catalog, other.catalog)
                    .append(schema, other.schema).append(tables, other.tables).isEquals();
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(name).append(tables).toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Database [name=").append(name);
        result.append("; catalog=").append(catalog);
        result.append("; schema=").append(schema);
        result.append("; tableCount=").append(getTableCount());
        result.append("]");

        return result.toString();
    }

    /**
     * Returns a verbose string representation of this database.
     * 
     * @return The string representation
     */
    public String toVerboseString() {
        StringBuffer result = new StringBuffer();

        result.append("Database [");
        result.append(getName());
        result.append("] tables:");
        for (int idx = 0; idx < getTableCount(); idx++) {
            result.append(" ");
            result.append(getTable(idx).toVerboseString());
        }

        return result.toString();
    }
}