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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Represents the database model, ie. the tables in the database. It also
 * contains the corresponding dyna classes for creating dyna beans for the
 * objects stored in the tables.
 */
public class Database implements Serializable, Cloneable {

    /** Unique ID for serialization purposes. */
    private static final long serialVersionUID = -3160443396757573868L;

    /** The name of the database model. */
    private String name;

    /** The method for generating primary keys (currently ignored). */
    private String idMethod;

    /** The version of the model. */
    private String version;

    /** The tables. */
    private ArrayList<Table> tables = new ArrayList<Table>();

    private Map<String, Integer> tableIndexCache = new HashMap<String, Integer>();

    public static Table[] sortByForeignKeys(Table... tables) {
        if (tables != null) {
            List<Table> list = new ArrayList<Table>(tables.length);
            for (Table table : tables) {
                list.add(table);
            }
            list = sortByForeignKeys(list);
            tables = list.toArray(new Table[list.size()]);
        }
        return tables;
    }

    public static List<Table> sortByForeignKeys(List<Table> tables) {
        List<Table> unsorted = new ArrayList<Table>(tables);
        List<Table> sorted = new ArrayList<Table>(unsorted.size());
        int index = 0;
        boolean sortedAtLeastOne = false;
        while (unsorted.size() > 0) {
            Table unsortedTable = unsorted.get(index);
            for (int i = 0; i < sorted.size() && !sorted.contains(unsortedTable); i++) {
                Table sortedTable = sorted.get(i);
                ForeignKey[] sortedFks = sortedTable.getForeignKeys();
                if (sortedFks != null) {
                    for (ForeignKey sortedFk : sortedFks) {
                        String fkTableName = sortedFk.getForeignTableName();
                        if (StringUtils.equals(fkTableName, unsortedTable.getName())) {
                            sorted.add(i, unsortedTable);
                            unsorted.remove(unsortedTable);
                            sortedAtLeastOne = true;
                            break;
                        }
                    }
                }

                ForeignKey[] unsortedFks = unsortedTable.getForeignKeys();
                if (unsortedFks != null) {
                    for (ForeignKey unsortedFk : unsortedFks) {
                        String fkTableName = unsortedFk.getForeignTableName();
                        if (StringUtils.equals(fkTableName, sortedTable.getName())) {
                            sorted.add(i + 1, unsortedTable);
                            unsorted.remove(unsortedTable);
                            sortedAtLeastOne = true;
                            break;
                        }
                    }
                }

            }

            index++;
            if (index >= unsorted.size()) {
                if (!sortedAtLeastOne) {
                    sorted.add(unsorted.remove(0));
                }
                index = 0;
                sortedAtLeastOne = false;
            }
        }
        return sorted;

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

                    if (targetTable == null) {
                        throw new ModelException("The foreignkey " + fkDesc + " in table "
                                + curTable.getName() + " references the undefined table "
                                + fk.getForeignTableName());
                    } else {
                        fk.setForeignTable(targetTable);
                    }
                }
                for (int refIdx = 0; refIdx < fk.getReferenceCount(); refIdx++) {
                    Reference ref = fk.getReference(refIdx);

                    if (ref.getLocalColumn() == null) {
                        Column localColumn = curTable.findColumn(ref.getLocalColumnName(), true);

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

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public Object clone() throws CloneNotSupportedException {
        Database result = (Database) super.clone();
        result.name = name;
        result.idMethod = idMethod;
        result.version = version;
        result.tables = (ArrayList<Table>) tables.clone();

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {
        if (obj instanceof Database) {
            Database other = (Database) obj;

            // Note that this compares case sensitive
            return new EqualsBuilder().append(name, other.name).append(tables, other.tables)
                    .isEquals();
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

        result.append("Database [name=");
        result.append(getName());
        result.append("; ");
        result.append(getTableCount());
        result.append(" tables]");

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
