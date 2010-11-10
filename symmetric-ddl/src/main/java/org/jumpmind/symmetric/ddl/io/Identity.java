package org.jumpmind.symmetric.ddl.io;

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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Stores the identity of an database object as defined by its primary keys. Is used
 * by {@link org.jumpmind.symmetric.ddl.io.DataToDatabaseSink} class for inserting objects
 * in the correct order. 
 * 
 * @version $Revision: 289996 $
 */
public class Identity
{
    /** The table. */
    private Table _table;
    /** The optional foreign key name whose referenced object this identity represents. */
    private String _fkName;
    /** The identity columns and their values. */
    private HashMap _columnValues = new HashMap();

    /**
     * Creates a new identity object for the given table.
     * 
     * @param table The name of the table
     */
    public Identity(Table table)
    {
        _table = table;
    }

    /**
     * Creates a new identity object for the given table.
     * 
     * @param table  The table
     * @param fkName The name of the foreign key whose referenced object this identity represents
     */
    public Identity(Table table, String fkName)
    {
        _table  = table;
        _fkName = fkName;
    }

    /**
     * Returns the table that this identity is for.
     * 
     * @return The table
     */
    public Table getTable()
    {
        return _table;
    }

    /**
     * Returns the name of the foreign key whose referenced object this identity represents. This
     * name is <code>null</code> if the identity is not for a foreign key, or if the foreign key
     * was unnamed.
     * 
     * @return The foreign key name
     */
    public String getForeignKeyName()
    {
        return _fkName;
    }
    
    /**
     * Specifies the value of the indicated identity columns.
     * 
     * @param name  The column name
     * @param value The value for the column
     */
    public void setColumnValue(String name, Object value)
    {
        _columnValues.put(name, value);
    }

    /**
     * Returns the value of the indicated identity columns.
     * 
     * @param name  The column name
     * @return The column's value
     */
    public Object getColumnValue(String name)
    {
        return _columnValues.get(name);
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj)
    {
        if (!(obj instanceof Identity))
        {
            return false;
        }

        Identity otherIdentity = (Identity)obj;

        if (!_table.equals(otherIdentity._table))
        {
            return false;
        }
        if (_columnValues.keySet().size() != otherIdentity._columnValues.keySet().size())
        {
            return false;
        }
        for (Iterator it = _columnValues.entrySet().iterator(); it.hasNext();)
        {
            Map.Entry entry      = (Map.Entry)it.next();
            Object    otherValue = otherIdentity._columnValues.get(entry.getKey());

            if (entry.getValue() == null)
            {
                if (otherValue != null)
                {
                    return false;
                }
            }
            else
            {
                if (!entry.getValue().equals(otherValue))
                {
                    return false;
                }
            }
        }
        
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return toString().hashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        StringBuffer buffer = new StringBuffer();

        buffer.append(_table.getName());
        buffer.append(":");
        for (Iterator it = _columnValues.entrySet().iterator(); it.hasNext();)
        {
            Map.Entry entry = (Map.Entry)it.next();

            buffer.append(entry.getKey());
            buffer.append("=");
            buffer.append(entry.getValue());
            if (it.hasNext())
            {
                buffer.append(";");
            }
        }
        return buffer.toString();
    }
}
