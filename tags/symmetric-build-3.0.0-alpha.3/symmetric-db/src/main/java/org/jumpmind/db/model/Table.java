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
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Represents a table in the database model.
 */
public class Table implements Serializable, Cloneable {

    /** Unique ID for serialization purposes. */
    private static final long serialVersionUID = -5541154961302342608L;

    /** The catalog of this table as read from the database. */
    private String catalog = null;

    /** The table's schema. */
    private String schema = null;

    /** The name. */
    private String name = null;

    /** A description of the table. */
    private String description = null;

    /** The table's type as read from the database. */
    private String type = null;

    /** The columns in this table. */
    private ArrayList<Column> columns = new ArrayList<Column>();

    /** The foreign keys associated to this table. */
    private ArrayList<ForeignKey> foreignKeys = new ArrayList<ForeignKey>();

    /** The indices applied to this table. */
    private ArrayList<IIndex> indices = new ArrayList<IIndex>();

    public Table() {
    }

    public Table(String tableName) {
        this(null, null, tableName);
    }

    public Table(String tableName, Column... columns) {
        this(null, null, tableName);
        if (columns != null) {
            for (Column column : columns) {
                addColumn(column);
            }
        }
    }

    public Table(String catalog, String schema, String tableName) {
        this.catalog = catalog;
        this.schema = schema;
        this.name = tableName;
    }

    public Table(String catalog, String schema, String tableName, String[] columnNames,
            String[] keyNames) {
        this(catalog, schema, tableName);
        for (String name : columnNames) {
            addColumn(new Column(name));
        }
        for (String name : keyNames) {
            getColumnWithName(name).setPrimaryKey(true);
        }

    }
    
    public void removeAllColumns() {
        columns.clear();
    }

    public void removeAllIndices() {
        indices.clear();
    }

    public void removeAllForeignKeys() {
        foreignKeys.clear();
    }

    /**
     * Returns the catalog of this table as read from the database.
     * 
     * @return The catalog
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * Sets the catalog of this table.
     * 
     * @param catalog
     *            The catalog
     */
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    /**
     * Returns the schema of this table as read from the database.
     * 
     * @return The schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * Sets the schema of this table.
     * 
     * @param schema
     *            The schema
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * Returns the type of this table as read from the database.
     * 
     * @return The type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type of this table.
     * 
     * @param type
     *            The type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Returns the name of the table.
     * 
     * @return The name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the table.
     * 
     * @param name
     *            The name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the description of the table.
     * 
     * @return The description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Sets the description of the table.
     * 
     * @param description
     *            The description
     */
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Returns the number of columns in this table.
     * 
     * @return The number of columns
     */
    public int getColumnCount() {
        return columns.size();
    }

    public int getPrimaryKeyColumnCount() {
        return getPrimaryKeyColumns().length;
    }

    /**
     * Returns the column at the specified position.
     * 
     * @param idx
     *            The column index
     * @return The column at this position
     */
    public Column getColumn(int idx) {
        return (Column) columns.get(idx);
    }

    /**
     * Returns the columns in this table.
     * 
     * @return The columns
     */
    public Column[] getColumns() {
        return (Column[]) columns.toArray(new Column[columns.size()]);
    }

    /**
     * Adds the given column.
     * 
     * @param column
     *            The column
     */
    public void addColumn(Column column) {
        if (column != null) {
            columns.add(column);
        }
    }

    /**
     * Adds the given column at the specified position.
     * 
     * @param idx
     *            The index where to add the column
     * @param column
     *            The column
     */
    public void addColumn(int idx, Column column) {
        if (column != null) {
            columns.add(idx, column);
        }
    }

    /**
     * Adds the column after the given previous column.
     * 
     * @param previousColumn
     *            The column to add the new column after; use <code>null</code>
     *            for adding at the begin
     * @param column
     *            The column
     */
    public void addColumn(Column previousColumn, Column column) {
        if (column != null) {
            if (previousColumn == null) {
                columns.add(0, column);
            } else {
                columns.add(columns.indexOf(previousColumn), column);
            }
        }
    }

    /**
     * Adds the given columns.
     * 
     * @param columns
     *            The columns
     */
    public void addColumns(Collection<Column> columns) {
        for (Iterator<Column> it = columns.iterator(); it.hasNext();) {
            addColumn((Column) it.next());
        }
    }

    /**
     * Removes the given column.
     * 
     * @param column
     *            The column to remove
     */
    public void removeColumn(Column column) {
        if (column != null) {
            columns.remove(column);
        }
    }

    /**
     * Removes the indicated column.
     * 
     * @param idx
     *            The index of the column to remove
     */
    public void removeColumn(int idx) {
        columns.remove(idx);
    }

    /**
     * Returns the number of foreign keys.
     * 
     * @return The number of foreign keys
     */
    public int getForeignKeyCount() {
        return foreignKeys.size();
    }

    /**
     * Returns the foreign key at the given position.
     * 
     * @param idx
     *            The foreign key index
     * @return The foreign key
     */
    public ForeignKey getForeignKey(int idx) {
        return (ForeignKey) foreignKeys.get(idx);
    }

    /**
     * Returns the foreign keys of this table.
     * 
     * @return The foreign keys
     */
    public ForeignKey[] getForeignKeys() {
        return (ForeignKey[]) foreignKeys.toArray(new ForeignKey[foreignKeys.size()]);
    }

    /**
     * Adds the given foreign key.
     * 
     * @param foreignKey
     *            The foreign key
     */
    public void addForeignKey(ForeignKey foreignKey) {
        if (foreignKey != null) {
            foreignKeys.add(foreignKey);
        }
    }

    /**
     * Adds the given foreign key at the specified position.
     * 
     * @param idx
     *            The index to add the foreign key at
     * @param foreignKey
     *            The foreign key
     */
    public void addForeignKey(int idx, ForeignKey foreignKey) {
        if (foreignKey != null) {
            foreignKeys.add(idx, foreignKey);
        }
    }

    /**
     * Adds the given foreign keys.
     * 
     * @param foreignKeys
     *            The foreign keys
     */
    public void addForeignKeys(Collection<ForeignKey> foreignKeys) {
        for (Iterator<ForeignKey> it = foreignKeys.iterator(); it.hasNext();) {
            addForeignKey((ForeignKey) it.next());
        }
    }

    /**
     * Removes the given foreign key.
     * 
     * @param foreignKey
     *            The foreign key to remove
     */
    public void removeForeignKey(ForeignKey foreignKey) {
        if (foreignKey != null) {
            foreignKeys.remove(foreignKey);
        }
    }

    /**
     * Removes the indicated foreign key.
     * 
     * @param idx
     *            The index of the foreign key to remove
     */
    public void removeForeignKey(int idx) {
        foreignKeys.remove(idx);
    }

    /**
     * Returns the number of indices.
     * 
     * @return The number of indices
     */
    public int getIndexCount() {
        return indices.size();
    }

    /**
     * Returns the index at the specified position.
     * 
     * @param idx
     *            The position
     * @return The index
     */
    public IIndex getIndex(int idx) {
        return (IIndex) indices.get(idx);
    }

    /**
     * Adds the given index.
     * 
     * @param index
     *            The index
     */
    public void addIndex(IIndex index) {
        if (index != null) {
            indices.add(index);
        }
    }

    /**
     * Adds the given index at the specified position.
     * 
     * @param idx
     *            The position to add the index at
     * @param index
     *            The index
     */
    public void addIndex(int idx, IIndex index) {
        if (index != null) {
            indices.add(idx, index);
        }
    }

    /**
     * Adds the given indices.
     * 
     * @param indices
     *            The indices
     */
    public void addIndices(Collection<IIndex> indices) {
        for (Iterator<IIndex> it = indices.iterator(); it.hasNext();) {
            addIndex((IIndex) it.next());
        }
    }

    /**
     * Returns the indices of this table.
     * 
     * @return The indices
     */
    public IIndex[] getIndices() {
        return (IIndex[]) indices.toArray(new IIndex[indices.size()]);
    }

    /**
     * Gets a list of non-unique indices on this table.
     * 
     * @return The unique indices
     */
    public IIndex[] getNonUniqueIndices() {
        if (indices != null) {
            List<IIndex> nonunique = new ArrayList<IIndex>();
            for (IIndex index : indices) {
                if (!index.isUnique()) {
                    nonunique.add(index);
                }
            }
            return nonunique.toArray(new IIndex[nonunique.size()]);
        } else {
            return new IIndex[0];
        }
    }

    /**
     * Gets a list of unique indices on this table.
     * 
     * @return The unique indices
     */
    public IIndex[] getUniqueIndices() {
        if (indices != null) {
            List<IIndex> unique = new ArrayList<IIndex>();
            for (IIndex index : indices) {
                if (index.isUnique()) {
                    unique.add(index);
                }
            }
            return unique.toArray(new IIndex[unique.size()]);
        } else {
            return new IIndex[0];
        }
    }

    /**
     * Removes the given index.
     * 
     * @param index
     *            The index to remove
     */
    public void removeIndex(IIndex index) {
        if (index != null) {
            indices.remove(index);
        }
    }

    /**
     * Removes the indicated index.
     * 
     * @param idx
     *            The position of the index to remove
     */
    public void removeIndex(int idx) {
        indices.remove(idx);
    }

    /**
     * Determines whether there is at least one primary key column on this
     * table.
     * 
     * @return <code>true</code> if there are one or more primary key columns
     */
    public boolean hasPrimaryKey() {
        for (Iterator<Column> it = columns.iterator(); it.hasNext();) {
            Column column = (Column) it.next();

            if (column.isPrimaryKey()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finds the column with the specified name, using case insensitive
     * matching. Note that this method is not called getColumn(String) to avoid
     * introspection problems.
     * 
     * @param name
     *            The name of the column
     * @return The column or <code>null</code> if there is no such column
     */
    public Column findColumn(String name) {
        return findColumn(name, false);
    }

    /**
     * Finds the column with the specified name, using case insensitive
     * matching. Note that this method is not called getColumn(String) to avoid
     * introspection problems.
     * 
     * @param name
     *            The name of the column
     * @param caseSensitive
     *            Whether case matters for the names
     * @return The column or <code>null</code> if there is no such column
     */
    public Column findColumn(String name, boolean caseSensitive) {
        for (Iterator<Column> it = columns.iterator(); it.hasNext();) {
            Column column = (Column) it.next();

            if (caseSensitive) {
                if (column.getName().equals(name)) {
                    return column;
                }
            } else {
                if (column.getName().equalsIgnoreCase(name)) {
                    return column;
                }
            }
        }
        return null;
    }

    /**
     * Determines the index of the given column.
     * 
     * @param column
     *            The column
     * @return The index or <code>-1</code> if it is no column of this table
     */
    public int getColumnIndex(Column column) {
        return getColumnIndex(column.getName());
    }

    public int getColumnIndex(String columnName) {
        int idx = 0;
        for (Iterator<Column> it = columns.iterator(); it.hasNext(); idx++) {
            if (columnName != null && columnName.equals(it.next().getName())) {
                return idx;
            }
        }
        return -1;
    }

    /**
     * Finds the index with the specified name, using case insensitive matching.
     * Note that this method is not called getIndex to avoid introspection
     * problems.
     * 
     * @param name
     *            The name of the index
     * @return The index or <code>null</code> if there is no such index
     */
    public IIndex findIndex(String name) {
        return findIndex(name, false);
    }

    /**
     * Finds the index with the specified name, using case insensitive matching.
     * Note that this method is not called getIndex to avoid introspection
     * problems.
     * 
     * @param name
     *            The name of the index
     * @param caseSensitive
     *            Whether case matters for the names
     * @return The index or <code>null</code> if there is no such index
     */
    public IIndex findIndex(String name, boolean caseSensitive) {
        for (int idx = 0; idx < getIndexCount(); idx++) {
            IIndex index = getIndex(idx);

            if (caseSensitive) {
                if (index.getName().equals(name)) {
                    return index;
                }
            } else {
                if (index.getName().equalsIgnoreCase(name)) {
                    return index;
                }
            }
        }
        return null;
    }

    /**
     * Finds the foreign key in this table that is equal to the supplied foreign
     * key.
     * 
     * @param key
     *            The foreign key to search for
     * @return The found foreign key
     */
    public ForeignKey findForeignKey(ForeignKey key) {
        for (int idx = 0; idx < getForeignKeyCount(); idx++) {
            ForeignKey fk = getForeignKey(idx);

            if (fk.equals(key)) {
                return fk;
            }
        }
        return null;
    }

    /**
     * Finds the foreign key in this table that is equal to the supplied foreign
     * key.
     * 
     * @param key
     *            The foreign key to search for
     * @param caseSensitive
     *            Whether case matters for the names
     * @return The found foreign key
     */
    public ForeignKey findForeignKey(ForeignKey key, boolean caseSensitive) {
        for (int idx = 0; idx < getForeignKeyCount(); idx++) {
            ForeignKey fk = getForeignKey(idx);

            if ((caseSensitive && fk.equals(key)) || (!caseSensitive && fk.equalsIgnoreCase(key))) {
                return fk;
            }
        }
        return null;
    }

    /**
     * Returns the foreign key referencing this table if it exists.
     * 
     * @return The self-referencing foreign key if any
     */
    public ForeignKey getSelfReferencingForeignKey() {
        for (int idx = 0; idx < getForeignKeyCount(); idx++) {
            ForeignKey fk = getForeignKey(idx);

            if (this.equals(fk.getForeignTable())) {
                return fk;
            }
        }
        return null;
    }

    /**
     * Returns the primary key columns of this table.
     * 
     * @return The primary key columns
     */
    public Column[] getPrimaryKeyColumns() {
        if (columns != null) {
            List<Column> selectedColumns = new ArrayList<Column>();
            for (Column column : columns) {
                if (column.isPrimaryKey()) {
                    selectedColumns.add(column);
                }
            }
            return selectedColumns.toArray(new Column[selectedColumns.size()]);
        } else {
            return new Column[0];
        }
    }

    /**
     * Returns the auto increment columns in this table. If no incrementcolumns
     * are found, it will return an empty array.
     * 
     * @return The columns
     */
    public Column[] getAutoIncrementColumns() {
        if (columns != null) {
            List<Column> selectedColumns = new ArrayList<Column>();
            for (Column column : columns) {
                if (column.isAutoIncrement()) {
                    selectedColumns.add(column);
                }
            }
            return selectedColumns.toArray(new Column[selectedColumns.size()]);
        } else {
            return new Column[0];
        }
    }

    /**
     * Sorts the foreign keys alphabetically.
     * 
     * @param caseSensitive
     *            Whether case matters
     */
    public void sortForeignKeys(final boolean caseSensitive) {
        if (!foreignKeys.isEmpty()) {
            final Collator collator = Collator.getInstance();

            Collections.sort(foreignKeys, new Comparator<ForeignKey>() {
                public int compare(ForeignKey obj1, ForeignKey obj2) {
                    String fk1Name = ((ForeignKey) obj1).getName();
                    String fk2Name = ((ForeignKey) obj2).getName();

                    if (!caseSensitive) {
                        fk1Name = (fk1Name != null ? fk1Name.toLowerCase() : null);
                        fk2Name = (fk2Name != null ? fk2Name.toLowerCase() : null);
                    }
                    return collator.compare(fk1Name, fk2Name);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object clone() throws CloneNotSupportedException {
        Table result = (Table) super.clone();
        result.catalog = catalog;
        result.schema = schema;
        result.name = name;
        result.type = type;
        result.columns = (ArrayList<Column>) columns.clone();
        result.foreignKeys = (ArrayList<ForeignKey>) foreignKeys.clone();
        result.indices = (ArrayList<IIndex>) indices.clone();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Table) {
            Table other = (Table) obj;

            // Note that this compares case sensitive
            // TODO: For now we ignore catalog and schema (type should be
            // irrelevant anyways)
            return new EqualsBuilder()
                    .append(name, other.name)
                    .append(columns, other.columns)
                    .append(new HashSet<ForeignKey>(foreignKeys),
                            new HashSet<ForeignKey>(other.foreignKeys))
                    .append(new HashSet<IIndex>(indices), new HashSet<IIndex>(other.indices))
                    .isEquals();
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        // TODO: For now we ignore catalog and schema (type should be irrelevant
        // anyways)
        return new HashCodeBuilder(17, 37).append(name).append(columns)
                .append(new HashSet<ForeignKey>(foreignKeys)).append(new HashSet<IIndex>(indices))
                .toHashCode();
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Table [name=");
        result.append(getName());
        result.append("; ");
        result.append(getColumnCount());
        result.append(" columns]");

        return result.toString();
    }

    /**
     * Returns a verbose string representation of this table.
     * 
     * @return The string representation
     */
    public String toVerboseString() {
        StringBuffer result = new StringBuffer();

        result.append("Table [name=");
        result.append(getName());
        result.append("; catalog=");
        result.append(getCatalog());
        result.append("; schema=");
        result.append(getSchema());
        result.append("; type=");
        result.append(getType());
        result.append("] columns:");
        for (int idx = 0; idx < getColumnCount(); idx++) {
            result.append(" ");
            result.append(getColumn(idx).toVerboseString());
        }
        result.append("; indices:");
        for (int idx = 0; idx < getIndexCount(); idx++) {
            result.append(" ");
            result.append(getIndex(idx).toVerboseString());
        }
        result.append("; foreign keys:");
        for (int idx = 0; idx < getForeignKeyCount(); idx++) {
            result.append(" ");
            result.append(getForeignKey(idx).toVerboseString());
        }

        return result.toString();
    }

    public String getFullyQualifiedTableName() {
        return getFullyQualifiedTableName(catalog, schema, name, null);
    }

    public static String getFullyQualifiedTableName(String catalogName, String schemaName,
            String tableName) {
        return getFullyQualifiedTableName(catalogName, schemaName, tableName, null);
    }

    public static String getQualifiedTablePrefix(String catalogName, String schemaName) {
        return getQualifiedTablePrefix(catalogName, schemaName, null);
    }

    public String getFullyQualifiedTableName(String quoteString) {
        return getFullyQualifiedTableName(catalog, schema, name, quoteString);
    }

    public static String getFullyQualifiedTableName(String catalogName, String schemaName,
            String tableName, String quoteString) {
        if (quoteString == null) {
            quoteString = "";
        }
        return getQualifiedTablePrefix(catalogName, schemaName, quoteString) + quoteString
                + tableName + quoteString;
    }

    public static String getQualifiedTablePrefix(String catalogName, String schemaName,
            String quoteString) {
        if (quoteString == null) {
            quoteString = "";
        }
        String fullyQualified = "";
        if (!StringUtils.isBlank(schemaName)) {
            fullyQualified = quoteString + schemaName + quoteString + "." + fullyQualified;
        }
        if (!StringUtils.isBlank(catalogName)) {
            fullyQualified = quoteString + catalogName + quoteString + "." + fullyQualified;
        }
        return fullyQualified;
    }

    public String getQualifiedTablePrefix(String quoteString) {
        return getQualifiedTablePrefix(catalog, schema, quoteString);
    }

    public Column getColumnWithName(String name) {
        Column[] columns = getColumns();
        if (columns != null) {
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(name)) {
                    return column;
                }
            }
        }
        return null;
    }

    public boolean doesIndexContainOnlyPrimaryKeyColumns(IIndex index) {
        IndexColumn[] columns = index.getColumns();
        if (columns != null) {
            for (IndexColumn indexColumn : columns) {
                Column column = getColumnWithName(indexColumn.getName());
                if (column == null || !column.isPrimaryKey()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public boolean hasAutoIncrementColumn() {
        if (columns != null) {
            for (Column column : getColumns()) {
                if (column.isAutoIncrement()) {
                    return true;
                }
            }
        }
        return false;
    }

    public Column[] getDistributionKeyColumns() {
        if (columns != null) {
            List<Column> selectedColumns = new ArrayList<Column>();
            for (Column column : columns) {
                if (column.isDistributionKey()) {
                    selectedColumns.add(column);
                }
            }
            return selectedColumns.toArray(new Column[selectedColumns.size()]);
        } else {
            return new Column[0];
        }
    }

    public void reOrderColumns(Column[] targetOrder, boolean copyPrimaryKeys) {
        ArrayList<Column> orderedColumns = new ArrayList<Column>(targetOrder.length);
        for (int i = 0; i < targetOrder.length; i++) {
            String name = targetOrder[i].getName();
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(name)) {
                    orderedColumns.add(column);
                    if (copyPrimaryKeys) {
                        column.setPrimaryKey(targetOrder[i].isPrimaryKey());
                    }
                    break;
                }
            }
        }
        columns = orderedColumns;
    }

    public void orderColumns(String[] columnNames) {
        Column[] orderedColumns = orderColumns(columnNames, this);
        this.columns.clear();
        for (Column column : orderedColumns) {
            this.columns.add(column);
        }
    }

    public static Column[] orderColumns(String[] columnNames, Table table) {
        Column[] unorderedColumns = table.getColumns();
        Column[] orderedColumns = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            String name = columnNames[i];
            for (Column column : unorderedColumns) {
                if (column.getName().equalsIgnoreCase(name)) {
                    orderedColumns[i] = column;
                    break;
                }
            }
        }
        return orderedColumns;
    }

    @SuppressWarnings("unchecked")
    public Table copy() {
        try {
            Table result = (Table) super.clone();
            result.catalog = catalog;
            result.schema = schema;
            result.name = name;
            result.type = type;
            result.columns = (ArrayList<Column>) columns.clone();
            result.foreignKeys = (ArrayList<ForeignKey>) foreignKeys.clone();
            result.indices = (ArrayList<IIndex>) indices.clone();
            return result;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String[] getColumnNames() {
        String[] columnNames = new String[columns.size()];
        int i = 0;
        for (Column col : columns) {
            columnNames[i++] = col.getName();
        }
        return columnNames;
    }

    public String[] getPrimaryKeyColumnNames() {
        Column[] columns = getPrimaryKeyColumns();
        String[] columnNames = new String[columns.length];
        int i = 0;
        for (Column col : columns) {
            columnNames[i++] = col.getName();
        }
        return columnNames;
    }
    
    public int calculateTableHashcode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + name.hashCode();
        result = PRIME * result + calculateHashcodeForColumns(PRIME, getColumns());
        result = PRIME * result + calculateHashcodeForColumns(PRIME, getPrimaryKeyColumns());
        return result;
    }
    
    private static int calculateHashcodeForColumns(final int PRIME, Column[] cols) {
        int result = 1;
        if (cols != null && cols.length > 0) {
            for (Column column : cols) {
                result = PRIME * result + column.getName().hashCode();
                result = PRIME * result + column.getType().hashCode();
                result = PRIME * result + column.getSizeAsInt();
            }
        }
        return result;
    }


    public static boolean areAllColumnsPrimaryKeys(Column[] columns) {
        boolean allPks = true;
        if (columns != null) {
            for (Column column : columns) {
                allPks &= column.isPrimaryKey();
            }
        }
        return allPks;
    }

    public static Table buildTable(String tableName, String[] keyNames, String[] columnNames) {
        Table table = new Table();
        table.setName(tableName);
        for (int i = 0; i < columnNames.length; i++) {
            table.addColumn(new Column(columnNames[i]));
        }
        for (int i = 0; i < keyNames.length; i++) {
            table.getColumnWithName(keyNames[i]).setPrimaryKey(true);
        }
        return table;
    }

    public static String getCommaDeliminatedColumns(Column[] cols) {
        StringBuilder columns = new StringBuilder();
        if (cols != null && cols.length > 0) {
            for (Column column : cols) {
                columns.append(column.getName());
                columns.append(",");
            }
            columns.replace(columns.length() - 1, columns.length(), "");
            return columns.toString();
        } else {
            return " ";
        }
    }

}
