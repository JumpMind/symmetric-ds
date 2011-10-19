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

import java.util.ArrayList;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Represents an index definition for a table.
 * 
 * @version $Revision: 289996 $
 */
public class NonUniqueIndex extends IndexImpBase
{
    /** Unique ID for serialization purposes. */
    private static final long serialVersionUID = -3591499395114850301L;

    /**
     * {@inheritDoc}
     */
    public boolean isUnique()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException
    {
        NonUniqueIndex result = new NonUniqueIndex();

        result._name    = _name;
        result._columns = (ArrayList)_columns.clone();

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj)
    {
        if (obj instanceof NonUniqueIndex)
        {
            NonUniqueIndex other = (NonUniqueIndex)obj;

            return new EqualsBuilder().append(_name,    other._name)
                                      .append(_columns, other._columns)
                                      .isEquals();
        }
        else
        {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean equalsIgnoreCase(Index other)
    {
        if (other instanceof NonUniqueIndex)
        {
            NonUniqueIndex otherIndex = (NonUniqueIndex)other;

            boolean checkName = (_name != null) && (_name.length() > 0) &&
                                (otherIndex._name != null) && (otherIndex._name.length() > 0);

            if ((!checkName || _name.equalsIgnoreCase(otherIndex._name)) &&
                (getColumnCount() == otherIndex.getColumnCount()))
            {
                for (int idx = 0; idx < getColumnCount(); idx++)
                {
                    if (!getColumn(idx).equalsIgnoreCase(otherIndex.getColumn(idx)))
                    {
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return new HashCodeBuilder(17, 37).append(_name)
                                          .append(_columns)
                                          .toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuffer result = new StringBuffer();

        result.append("Index [name=");
        result.append(getName());
        result.append("; ");
        result.append(getColumnCount());
        result.append(" columns]");

        return result.toString();
    }

    /**
     * {@inheritDoc}
     */
    public String toVerboseString()
    {
        StringBuffer result = new StringBuffer();

        result.append("Index [");
        result.append(getName());
        result.append("] columns:");
        for (int idx = 0; idx < getColumnCount(); idx++)
        {
            result.append(" ");
            result.append(getColumn(idx).toString());
        }

        return result.toString();
    }
}
