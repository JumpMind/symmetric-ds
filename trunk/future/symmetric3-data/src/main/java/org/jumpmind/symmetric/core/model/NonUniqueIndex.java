package org.jumpmind.symmetric.core.model;

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

/**
 * Represents an index definition for a table.
 * 
 * @version $Revision: 289996 $
 */
public class NonUniqueIndex extends Index
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
    @SuppressWarnings("unchecked")
    public Object clone() throws CloneNotSupportedException
    {
        NonUniqueIndex result = new NonUniqueIndex();

        result._name    = _name;
        result._columns = (ArrayList<IndexColumn>)_columns.clone();

        return result;
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
