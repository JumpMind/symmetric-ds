package org.jumpmind.symmetric.ddl.dynabean;

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

import org.apache.commons.beanutils.DynaProperty;
import org.jumpmind.symmetric.ddl.model.Column;

/**
 * A DynaProperty which maps to a persistent Column in a database.
 * The Column describes additional relational metadata 
 * for the property such as whether the property is a primary key column, 
 * an autoIncrement column and the SQL type etc.
 *
 * @version $Revision: 463757 $
 */
public class SqlDynaProperty extends DynaProperty
{
    /** Unique ID for serializaion purposes. */
    private static final long serialVersionUID = -4491018827449106588L;

    /** The column for which this dyna property is defined. */
    private Column _column;    

    /**
     * Creates a property instance for the given column that accepts any data type.
     *
     * @param column The column
     */
    public SqlDynaProperty(Column column)
    {
        super(column.getName());
        _column = column;
    }

    /**
     * Creates a property instance for the given column that only accepts the given type.
     *
     * @param column The column
     * @param type   The type of the property
     */
    public SqlDynaProperty(Column column, Class type)
    {
        super(column.getName(), type);
        _column = column;
    }

    /**
     * Returns the column for which this property is defined.
     * 
     * @return The column
     */
    public Column getColumn()
    {
        return _column;
    }

    // Helper methods
    //-------------------------------------------------------------------------                
    
    /**
     * Determines whether this property is for a primary key column.
     * 
     * @return <code>true</code> if the property is for a primary key column
     */
    public boolean isPrimaryKey()
    {
        return getColumn().isPrimaryKey();
    }    
    
}
