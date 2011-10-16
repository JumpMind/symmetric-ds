package org.jumpmind.symmetric.ddl.model;

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

import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.collections.set.ListOrderedSet;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Represents a database foreign key.
 * 
 * @version $Revision: 504014 $
 */
public class ForeignKey implements Cloneable
{
    /** The name of the foreign key, may be <code>null</code>. */
    private String _name;
    /** The target table. */
    private Table _foreignTable;
    /** The name of the foreign table. */
    private String _foreignTableName;
    /** The references between local and remote columns. */
    private ListOrderedSet _references = new ListOrderedSet();
    /** Whether this foreign key has an associated auto-generated index. */
    private boolean _autoIndexPresent;

    /**
     * Creates a new foreign key object that has no name.
     */
    public ForeignKey()
    {
        this(null);
    }
    
    /**
     * Creates a new foreign key object.
     * 
     * @param name The name of the foreign key
     */
    public ForeignKey(String name)
    {
        _name = name;
    }

    /**
     * Returns the name of this foreign key.
     * 
     * @return The name
     */
    public String getName()
    {
        return _name;
    }

    /**
     * Sets the name of this foreign key.
     * 
     * @param name The name
     */
    public void setName(String name)
    {
        _name = name;
    }

    /**
     * Returns the foreign table.
     *
     * @return The foreign table
     */
    public Table getForeignTable()
    {
        return _foreignTable;
    }

    /**
     * Sets the foreign table.
     *
     * @param foreignTable The foreign table
     */
    public void setForeignTable(Table foreignTable)
    {
        _foreignTable     = foreignTable;
        _foreignTableName = (foreignTable == null ? null : foreignTable.getName());
    }

    /**
     * Returns the name of the foreign table.
     * 
     * @return The table name
     */
    public String getForeignTableName()
    {
        return _foreignTableName;
    }
    
    /**
     * Sets the name of the foreign table. Please note that you should not use this method
     * when manually constructing or manipulating the database model. Rather utilize the
     * {@link #setForeignTable(Table)} method.
     * 
     * @param foreignTableName The table name
     */
    public void setForeignTableName(String foreignTableName)
    {
        if ((_foreignTable != null) && !_foreignTable.getName().equals(foreignTableName))
        {
            _foreignTable = null;
        }
        _foreignTableName = foreignTableName;
    }

    /**
     * Returns the number of references.
     * 
     * @return The number of references
     */
    public int getReferenceCount()
    {
        return _references.size();
    }

    /**
     * Returns the indicated reference.
     * 
     * @param idx The index
     * @return The reference
     */
    public Reference getReference(int idx)
    {
        return (Reference)_references.get(idx);
    }

    /**
     * Returns the references.
     * 
     * @return The references
     */
    public Reference[] getReferences()
    {
        return (Reference[])_references.toArray(new Reference[_references.size()]);
    }

    /**
     * Returns the first reference if it exists.
     * 
     * @return The first reference
     */
    public Reference getFirstReference()
    {
        return (Reference)(_references.isEmpty() ? null : _references.get(0));
    }

    /**
     * Adds a reference, ie. a mapping between a local column (in the table that owns this foreign key)
     * and a remote column.
     * 
     * @param reference The reference to add
     */
    public void addReference(Reference reference)
    {
        if (reference != null)
        {
            for (int idx = 0; idx < _references.size(); idx++)
            {
                Reference curRef = getReference(idx);

                if (curRef.getSequenceValue() > reference.getSequenceValue())
                {
                    _references.add(idx, reference);
                    return;
                }
            }
            _references.add(reference);
        }
    }

    /**
     * Removes the given reference.
     * 
     * @param reference The reference to remove
     */
    public void removeReference(Reference reference)
    {
        if (reference != null)
        {
            _references.remove(reference);
        }
    }

    /**
     * Removes the indicated reference.
     * 
     * @param idx The index of the reference to remove
     */
    public void removeReference(int idx)
    {
        _references.remove(idx);
    }

    /**
     * Determines whether this foreign key uses the given column as a local
     * column in a reference.
     * 
     * @param column The column to check
     * @return <code>true</code> if a reference uses the column as a local
     *         column
     */
    public boolean hasLocalColumn(Column column)
    {
        for (int idx = 0; idx < getReferenceCount(); idx++)
        {
            if (column.equals(getReference(idx).getLocalColumn()))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Determines whether this foreign key uses the given column as a foreign
     * column in a reference.
     * 
     * @param column The column to check
     * @return <code>true</code> if a reference uses the column as a foreign
     *         column
     */
    public boolean hasForeignColumn(Column column)
    {
        for (int idx = 0; idx < getReferenceCount(); idx++)
        {
            if (column.equals(getReference(idx).getForeignColumn()))
            {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Determines whether this foreign key has an auto-generated associated index.
     * 
     * @return <code>true</code> if an auto-generated index exists
     */
    public boolean isAutoIndexPresent()
    {
        return _autoIndexPresent;
    }

    /**
     * Specifies whether this foreign key has an auto-generated associated index.
     * 
     * @param autoIndexPresent <code>true</code> if an auto-generated index exists
     */
    public void setAutoIndexPresent(boolean autoIndexPresent)
    {
        _autoIndexPresent = autoIndexPresent;
    }

    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException
    {
        ForeignKey result = (ForeignKey)super.clone();

        result._name             = _name;
        result._foreignTableName = _foreignTableName;
        result._references       = new ListOrderedSet();

        for (Iterator it = _references.iterator(); it.hasNext();)
        {
            result._references.add(((Reference)it.next()).clone());
        }

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj)
    {
        if (obj instanceof ForeignKey)
        {
            ForeignKey otherFk = (ForeignKey)obj;

            // Note that this compares case sensitive
            // Note also that we can simply compare the references regardless of their order
            // (which is irrelevant for fks) because they are contained in a set
            EqualsBuilder builder = new EqualsBuilder();

            if ((_name != null) && (_name.length() > 0) && (otherFk._name != null) && (otherFk._name.length() > 0))
            {
                builder.append(_name, otherFk._name);
            }
            return builder.append(_foreignTableName, otherFk._foreignTableName)
                          .append(_references,       otherFk._references)
                          .isEquals();
        }
        else
        {
            return false;
        }
    }

    /**
     * Compares this foreign key to the given one while ignoring the case of identifiers.
     * 
     * @param otherFk The other foreign key
     * @return <code>true</code> if this foreign key is equal (ignoring case) to the given one
     */
    public boolean equalsIgnoreCase(ForeignKey otherFk)
    {
        boolean checkName = (_name != null) && (_name.length() > 0) &&
                            (otherFk._name != null) && (otherFk._name.length() > 0);

        if ((!checkName || _name.equalsIgnoreCase(otherFk._name)) &&
            _foreignTableName.equalsIgnoreCase(otherFk._foreignTableName))
        {
            HashSet otherRefs = new HashSet();

            otherRefs.addAll(otherFk._references);
            for (Iterator it = _references.iterator(); it.hasNext();)
            {
                Reference curLocalRef = (Reference)it.next();
                boolean   found       = false;

                for (Iterator otherIt = otherRefs.iterator(); otherIt.hasNext();)
                {
                    Reference curOtherRef = (Reference)otherIt.next();

                    if (curLocalRef.equalsIgnoreCase(curOtherRef))
                    {
                        otherIt.remove();
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    return false;
                }
            }
            return otherRefs.isEmpty();
        }
        else
        {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return new HashCodeBuilder(17, 37).append(_name)
                                          .append(_foreignTableName)
                                          .append(_references)
                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuffer result = new StringBuffer();

        result.append("Foreign key [");
        if ((getName() != null) && (getName().length() > 0))
        {
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
    public String toVerboseString()
    {
        StringBuffer result = new StringBuffer();

        result.append("ForeignK ky [");
        if ((getName() != null) && (getName().length() > 0))
        {
            result.append("name=");
            result.append(getName());
            result.append("; ");
        }
        result.append("foreign table=");
        result.append(getForeignTableName());
        result.append("] references:");
        for (int idx = 0; idx < getReferenceCount(); idx++)
        {
            result.append(" ");
            result.append(getReference(idx).toString());
        }

        return result.toString();
    }
}
