package org.jumpmind.symmetric.core.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a database foreign key.
 */
public class ForeignKey implements Cloneable {

    /** The name of the foreign key, may be <code>null</code>. */
    private String name;

    /** The target table. */
    private Table foreignTable;

    /** The name of the foreign table. */
    private String foreignTableName;

    /** The references between local and remote columns. */
    private List<Reference> references = new ArrayList<Reference>();

    /** Whether this foreign key has an associated auto-generated index. */
    private boolean autoIndexPresent;

    /**
     * Creates a new foreign key object that has no name.
     */
    public ForeignKey() {
        this(null);
    }

    /**
     * Creates a new foreign key object.
     * 
     * @param name
     *            The name of the foreign key
     */
    public ForeignKey(String name) {
        this.name = name;
    }

    /**
     * Returns the name of this foreign key.
     * 
     * @return The name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this foreign key.
     * 
     * @param name
     *            The name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the foreign table.
     * 
     * @return The foreign table
     */
    public Table getForeignTable() {
        return foreignTable;
    }

    /**
     * Sets the foreign table.
     * 
     * @param foreignTable
     *            The foreign table
     */
    public void setForeignTable(Table foreignTable) {
        this.foreignTable = foreignTable;
        this.foreignTableName = (foreignTable == null ? null : foreignTable.getName());
    }

    /**
     * Returns the name of the foreign table.
     * 
     * @return The table name
     */
    public String getForeignTableName() {
        return foreignTableName;
    }

    /**
     * Sets the name of the foreign table. Please note that you should not use
     * this method when manually constructing or manipulating the database
     * model. Rather utilize the {@link #setForeignTable(Table)} method.
     * 
     * @param foreignTableName
     *            The table name
     */
    public void setForeignTableName(String foreignTableName) {
        if ((foreignTable != null) && !foreignTable.getName().equals(foreignTableName)) {
            foreignTable = null;
        }
        this.foreignTableName = foreignTableName;
    }

    /**
     * Returns the number of references.
     * 
     * @return The number of references
     */
    public int getReferenceCount() {
        return references.size();
    }

    /**
     * Returns the indicated reference.
     * 
     * @param idx
     *            The index
     * @return The reference
     */
    public Reference getReference(int idx) {
        return (Reference) references.get(idx);
    }

    /**
     * Returns the references.
     * 
     * @return The references
     */
    public Reference[] getReferences() {
        return (Reference[]) references.toArray(new Reference[references.size()]);
    }

    /**
     * Returns the first reference if it exists.
     * 
     * @return The first reference
     */
    public Reference getFirstReference() {
        return (Reference) (references.isEmpty() ? null : references.get(0));
    }

    /**
     * Adds a reference, ie. a mapping between a local column (in the table that
     * owns this foreign key) and a remote column.
     * 
     * @param reference
     *            The reference to add
     */
    public void addReference(Reference reference) {
        if (reference != null) {
            for (int idx = 0; idx < references.size(); idx++) {
                Reference curRef = getReference(idx);

                if (curRef.getSequenceValue() > reference.getSequenceValue()) {
                    references.add(idx, reference);
                    return;
                }
            }
            references.add(reference);
        }
    }

    /**
     * Removes the given reference.
     * 
     * @param reference
     *            The reference to remove
     */
    public void removeReference(Reference reference) {
        if (reference != null) {
            references.remove(reference);
        }
    }

    /**
     * Removes the indicated reference.
     * 
     * @param idx
     *            The index of the reference to remove
     */
    public void removeReference(int idx) {
        references.remove(idx);
    }

    /**
     * Determines whether this foreign key uses the given column as a local
     * column in a reference.
     * 
     * @param column
     *            The column to check
     * @return <code>true</code> if a reference uses the column as a local
     *         column
     */
    public boolean hasLocalColumn(Column column) {
        for (int idx = 0; idx < getReferenceCount(); idx++) {
            if (column.equals(getReference(idx).getLocalColumn())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines whether this foreign key uses the given column as a foreign
     * column in a reference.
     * 
     * @param column
     *            The column to check
     * @return <code>true</code> if a reference uses the column as a foreign
     *         column
     */
    public boolean hasForeignColumn(Column column) {
        for (int idx = 0; idx < getReferenceCount(); idx++) {
            if (column.equals(getReference(idx).getForeignColumn())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines whether this foreign key has an auto-generated associated
     * index.
     * 
     * @return <code>true</code> if an auto-generated index exists
     */
    public boolean isAutoIndexPresent() {
        return autoIndexPresent;
    }

    /**
     * Specifies whether this foreign key has an auto-generated associated
     * index.
     * 
     * @param autoIndexPresent
     *            <code>true</code> if an auto-generated index exists
     */
    public void setAutoIndexPresent(boolean autoIndexPresent) {
        this.autoIndexPresent = autoIndexPresent;
    }

    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException {
        ForeignKey result = (ForeignKey) super.clone();

        result.name = name;
        result.foreignTableName = foreignTableName;
        result.references = new ArrayList<Reference>();

        for (Iterator<Reference> it = references.iterator(); it.hasNext();) {
            result.references.add((Reference) (it.next()).clone());
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Foreign key [");
        if ((getName() != null) && (getName().length() > 0)) {
            result.append("name=");
            result.append(getName());
            result.append("; ");
        }
        result.append("foreign table=");
        result.append(getForeignTableName());
        result.append("; ");
        result.append(getReferenceCount());
        result.append(" references]");

        return result.toString();
    }

    /**
     * Returns a verbose string representation of this foreign key.
     * 
     * @return The string representation
     */
    public String toVerboseString() {
        StringBuffer result = new StringBuffer();

        result.append("ForeignK ky [");
        if ((getName() != null) && (getName().length() > 0)) {
            result.append("name=");
            result.append(getName());
            result.append("; ");
        }
        result.append("foreign table=");
        result.append(getForeignTableName());
        result.append("] references:");
        for (int idx = 0; idx < getReferenceCount(); idx++) {
            result.append(" ");
            result.append(getReference(idx).toString());
        }

        return result.toString();
    }
}
