package org.jumpmind.symmetric.core.model;

import java.io.Serializable;

import org.jumpmind.symmetric.core.common.EqualsBuilder;

/**
 * Represents a reference between a column in the local table and a column in
 * another table.
 */
public class Reference implements Cloneable, Serializable {
    /** Unique ID for serialization purposes. */
    private static final long serialVersionUID = 6062467640266171664L;

    /** The sequence value within the key. */
    private int sequenceValue;
    /** The local column. */
    private Column localColumn;
    /** The foreign column. */
    private Column foreignColumn;
    /** The name of the local column. */
    private String localColumnName;
    /** The name of the foreign column. */
    private String foreignColumnName;

    /**
     * Creates a new, empty reference.
     */
    public Reference() {
    }

    /**
     * Creates a new reference between the two given columns.
     * 
     * @param localColumn
     *            The local column
     * @param foreignColumn
     *            The remote column
     */
    public Reference(Column localColumn, Column foreignColumn) {
        setLocalColumn(localColumn);
        setForeignColumn(foreignColumn);
    }

    /**
     * Returns the sequence value within the owning key.
     * 
     * @return The sequence value
     */
    public int getSequenceValue() {
        return sequenceValue;
    }

    /**
     * Sets the sequence value within the owning key. Please note that you
     * should not change the value once the reference has been added to a key.
     * 
     * @param sequenceValue
     *            The sequence value
     */
    public void setSequenceValue(int sequenceValue) {
        this.sequenceValue = sequenceValue;
    }

    /**
     * Returns the local column.
     * 
     * @return The local column
     */
    public Column getLocalColumn() {
        return localColumn;
    }

    /**
     * Sets the local column.
     * 
     * @param localColumn
     *            The local column
     */
    public void setLocalColumn(Column localColumn) {
        this.localColumn = localColumn;
        localColumnName = (localColumn == null ? null : localColumn.getName());
    }

    /**
     * Returns the foreign column.
     * 
     * @return The foreign column
     */
    public Column getForeignColumn() {
        return foreignColumn;
    }

    /**
     * Sets the foreign column.
     * 
     * @param foreignColumn
     *            The foreign column
     */
    public void setForeignColumn(Column foreignColumn) {
        this.foreignColumn = foreignColumn;
        foreignColumnName = (foreignColumn == null ? null : foreignColumn.getName());
    }

    /**
     * Returns the name of the local column.
     * 
     * @return The column name
     */
    public String getLocalColumnName() {
        return localColumnName;
    }

    /**
     * Sets the name of the local column. Note that you should not use this
     * method when manipulating the model manually. Rather use the
     * {@link #setLocalColumn(Column)} method.
     * 
     * @param localColumnName
     *            The column name
     */
    public void setLocalColumnName(String localColumnName) {
        if ((localColumn != null) && !localColumn.getName().equals(localColumnName)) {
            localColumn = null;
        }
        this.localColumnName = localColumnName;
    }

    /**
     * Returns the name of the foreign column.
     * 
     * @return The column name
     */
    public String getForeignColumnName() {
        return foreignColumnName;
    }

    /**
     * Sets the name of the remote column. Note that you should not use this
     * method when manipulating the model manually. Rather use the
     * {@link #setForeignColumn(Column)} method.
     * 
     * @param foreignColumnName
     *            The column name
     */
    public void setForeignColumnName(String foreignColumnName) {
        if ((foreignColumn != null) && !foreignColumn.getName().equals(foreignColumnName)) {
            foreignColumn = null;
        }
        this.foreignColumnName = foreignColumnName;
    }

    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException {
        Reference result = (Reference) super.clone();

        result.localColumnName = localColumnName;
        result.foreignColumnName = foreignColumnName;

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {
        if (obj instanceof Reference) {
            Reference other = (Reference) obj;

            return new EqualsBuilder().append(localColumnName, other.localColumnName)
                    .append(foreignColumnName, other.foreignColumnName).isEquals();
        } else {
            return false;
        }
    }

    /**
     * Compares this reference to the given one while ignoring the case of
     * identifiers.
     * 
     * @param otherRef
     *            The other reference
     * @return <code>true</code> if this reference is equal (ignoring case) to
     *         the given one
     */
    public boolean equalsIgnoreCase(Reference otherRef) {
        return (otherRef != null) && localColumnName.equalsIgnoreCase(otherRef.localColumnName)
                && foreignColumnName.equalsIgnoreCase(otherRef.foreignColumnName);
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append(getLocalColumnName());
        result.append(" -> ");
        result.append(getForeignColumnName());

        return result.toString();
    }
}
