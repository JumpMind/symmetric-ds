package org.jumpmind.symmetric.core.model;

import java.io.Serializable;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.core.common.EqualsBuilder;
import org.jumpmind.symmetric.core.common.HashCodeBuilder;
import org.jumpmind.symmetric.core.common.StringUtils;

/**
 * Represents a relational database table
 */
public class Table implements Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    /** The catalog of this table as read from the database. */
    private String catalogName = null;

    /** The table's schema. */
    private String schemaName = null;

    /** The name. */
    private String tableName = null;

    /** A description of the table. */
    private String description = null;

    /** The table's type as read from the database. */
    private String type = null;

    /** The columns in this table. */
    private ArrayList<Column> columns = new ArrayList<Column>();

    /** The foreign keys associated to this table. */
    private ArrayList<ForeignKey> foreignKeys = new ArrayList<ForeignKey>();

    /** The indices applied to this table. */
    private ArrayList<Index> indices = new ArrayList<Index>();

    public Table() {

    }

    public Table(String catalogName, String schemaName, String tableName) {
        this.setCatalogName(catalogName);
        this.setSchemaName(schemaName);
        this.setTableName(tableName);
    }

    public Table(String tableName) {
        this.setTableName(tableName);
    }

    public Table(String name, Column... columns) {
        this(name);
        if (columns != null) {
            for (Column column : columns) {
                addColumn(column);
            }
        }
    }

    /**
     * Returns the catalog of this table as read from the database.
     * 
     * @return The catalog
     */
    public String getCatalogName() {
        return catalogName;
    }

    /**
     * Sets the catalog of this table.
     * 
     * @param catalog
     *            The catalog
     */
    public void setCatalogName(String catalog) {
        this.catalogName = catalog;
    }

    /**
     * Returns the schema of this table as read from the database.
     * 
     * @return The schema
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Sets the schema of this table.
     * 
     * @param schema
     *            The schema
     */
    public void setSchemaName(String schema) {
        this.schemaName = schema;
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
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets the name of the table.
     * 
     * @param name
     *            The name
     */
    public void setTableName(String name) {
        this.tableName = name;
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
            addColumn(it.next());
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
            addForeignKey(it.next());
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
    public Index getIndex(int idx) {
        return (Index) indices.get(idx);
    }

    /**
     * Adds the given index.
     * 
     * @param index
     *            The index
     */
    public void addIndex(Index index) {
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
    public void addIndex(int idx, Index index) {
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
    public void addIndices(Collection<Index> indices) {
        for (Iterator<Index> it = indices.iterator(); it.hasNext();) {
            addIndex(it.next());
        }
    }

    /**
     * Returns the indices of this table.
     * 
     * @return The indices
     */
    public Index[] getIndices() {
        return (Index[]) indices.toArray(new Index[indices.size()]);
    }

    /**
     * Removes the given index.
     * 
     * @param index
     *            The index to remove
     */
    public void removeIndex(Index index) {
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
            Column column = it.next();

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
        int idx = 0;

        for (Iterator<Column> it = columns.iterator(); it.hasNext(); idx++) {
            if (column == it.next()) {
                return idx;
            }
        }
        return -1;
    }

    /**
     * Determines the index of the given column.
     * 
     * @param columnName
     * @return The index or <code>-1</code> if it is no column of this table
     */
    public int getColumnIndex(String columnName) {
        int idx = 0;
        for (Iterator<Column> it = columns.iterator(); it.hasNext(); idx++) {
            Column column = it.next();
            if (column.getName().equals(columnName)) {
                return idx;
            }
        }
        return -1;
    }

    public Column getColumn(String columnName) {
        int idx = 0;
        for (Iterator<Column> it = columns.iterator(); it.hasNext(); idx++) {
            Column column = it.next();
            if (column.getName().equals(columnName)) {
                return column;
            }
        }
        return null;
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
    public Index findIndex(String name) {
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
    public Index findIndex(String name, boolean caseSensitive) {
        for (int idx = 0; idx < getIndexCount(); idx++) {
            Index index = getIndex(idx);

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
    public List<Column> getPrimaryKeyColumns() {
        List<Column> found = new ArrayList<Column>();

        for (Column column : columns) {
            if (column.isPrimaryKey()) {
                found.add(column);
            }
        }
        return found;
    }

    public Column[] getPrimaryKeyColumnsArray() {
        List<Column> columns = getPrimaryKeyColumns();
        return columns.toArray(new Column[columns.size()]);
    }

    public Column[] getColumnSet(Set<String> columnNames, boolean onlyPrimaryKeys) {
        List<Column> found = new ArrayList<Column>();
        for (Column column : columns) {
            if ((onlyPrimaryKeys && column.isPrimaryKey()) || !onlyPrimaryKeys) {
                if (columnNames != null && columnNames.contains(column.getName())) {
                    found.add(column);
                }
            }
        }
        return found.toArray(new Column[found.size()]);
    }

    /**
     * Returns the auto increment columns in this table. If no incrementcolumns
     * are found, it will return an empty array.
     * 
     * @return The columns
     */
    public List<Column> getAutoIncrementColumns() {
        List<Column> found = new ArrayList<Column>(1);

        for (Column column : columns) {
            if (column.isAutoIncrement()) {
                found.add(column);
            }
        }
        return found;
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
                    String fk1Name = obj1.getName();
                    String fk2Name = obj2.getName();

                    if (!caseSensitive) {
                        fk1Name = (fk1Name != null ? fk1Name.toLowerCase() : null);
                        fk2Name = (fk2Name != null ? fk2Name.toLowerCase() : null);
                    }
                    return collator.compare(fk1Name, fk2Name);
                }
            });
        }
    }

    public void reOrderColumns(Column[] targetOrder, boolean copyPrimaryKeys) {
        ArrayList<Column> orderedColumns = new ArrayList<Column>(targetOrder.length);
        for (int i = 0; i < targetOrder.length; i++) {
            String name = targetOrder[i].getName();
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(name)) {
                    orderedColumns.add(i, column);
                    if (copyPrimaryKeys) {
                        column.setPrimaryKey(targetOrder[i].isPrimaryKey());
                    }
                    break;
                }
            }
        }
        columns = orderedColumns;
    }

    public String getFullyQualifiedTableName(String quoteString) {
        return getFullyQualifiedTableName(catalogName, schemaName, tableName, quoteString);
    }

    public static String getFullyQualifiedTableName(String catalogName, String schemaName,
            String tableName, String quoteString) {
        if (quoteString == null) {
            quoteString = "";
        }
        return getQualifiedTablePrefix(schemaName, catalogName, quoteString) + quoteString
                + tableName + quoteString;
    }

    public static String getQualifiedTablePrefix(String schemaName, String catalogName,
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
        return getQualifiedTablePrefix(schemaName, catalogName, quoteString);
    }

    public boolean hasUniqueIndexThatMatchesPrimaryKeys() {
        Index[] indexes = getIndices();
        if (indexes != null && indexes.length > 0) {
            for (Index index : indexes) {
                if (index instanceof UniqueIndex) {
                    boolean isPrimaryKey = true;
                    IndexColumn[] columns = index.getColumns();
                    if (columns != null && columns.length > 0) {
                        for (IndexColumn indexColumn : columns) {
                            isPrimaryKey &= getColumn(getColumnIndex(indexColumn.getName()))
                                    .isPrimaryKey();
                        }
                    }

                    if (isPrimaryKey) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public Table copy() {
        try {
            Table result = (Table) super.clone();
            result.catalogName = catalogName;
            result.schemaName = schemaName;
            result.tableName = tableName;
            result.type = type;
            result.columns = (ArrayList<Column>) columns.clone();
            result.foreignKeys = (ArrayList<ForeignKey>) foreignKeys.clone();
            result.indices = (ArrayList<Index>) indices.clone();
            return result;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException(ex);
        }
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

    public boolean doesIndexContainOnlyPrimaryKeyColumns(Index index) {
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

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {
        if (obj instanceof Table) {
            Table other = (Table) obj;

            // Note that this compares case sensitive
            return new EqualsBuilder()
                    .append(catalogName, other.catalogName)
                    .append(schemaName, other.schemaName)
                    .append(tableName, other.tableName)
                    .append(columns, other.columns)
                    .append(new HashSet<ForeignKey>(foreignKeys),
                            new HashSet<ForeignKey>(other.foreignKeys))
                    .append(new HashSet<Index>(indices), new HashSet<Index>(other.indices))
                    .isEquals();
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(tableName).append(catalogName).append(schemaName)
                .append(columns).append(new HashSet<ForeignKey>(foreignKeys))
                .append(new HashSet<Index>(indices)).toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Table [name=");
        result.append(getTableName());
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
        result.append(getTableName());
        result.append("; catalog=");
        result.append(getCatalogName());
        result.append("; schema=");
        result.append(getSchemaName());
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
}
