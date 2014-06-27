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

import java.io.Serializable;

/**
 * Represents an index definition for a table which may be either unique or non-unique.
 * 
 * @version $Revision: 504014 $
 */
public interface Index extends Cloneable, Serializable
{
    /**
     * Determines whether this index is unique or not.
     * 
     * @return <code>true</code> if the index is an unique one
     */
    public boolean isUnique();

    /**
     * Returns the name of the index.
     * 
     * @return The name
     */
    public String getName();
    
    /**
     * Sets the name of the index.
     * 
     * @param name The name
     */
    public void setName(String name);

    /**
     * Returns the number of columns that make up this index.
     * 
     * @return The number of index columns
     */
    public int getColumnCount();

    /**
     * Returns the indicated column making up this index.
     * 
     * @param idx The index of the column
     * @return The column
     */
    public IndexColumn getColumn(int idx);

    /**
     * Returns the columns that make up this index.
     * 
     * @return The columns
     */
    public IndexColumn[] getColumns();

    /**
     * Determines whether this index includes the given column.
     * 
     * @param column The column to check for
     * @return <code>true</code> if the column is included in this index
     */
    public boolean hasColumn(Column column);

    /**
     * Adds a column that makes up this index.
     * 
     * @param column The column to add
     */
    public void addColumn(IndexColumn column);

    /**
     * Removes the given index column from this index.
     * 
     * @param column The column to remove
     */
    public void removeColumn(IndexColumn column);

    /**
     * Removes the column at the specified position in this index.
     * 
     * @param idx The position of the index column to remove
     */
    public void removeColumn(int idx);

    /**
     * Clones this index.
     * 
     * @return The clone
     * @throws CloneNotSupportedException If the cloning did fail
     */
    public Object clone() throws CloneNotSupportedException;

    /**
     * Compares this index to the given one while ignoring the case of identifiers.
     * 
     * @param otherIndex The other index
     * @return <code>true</code> if this index is equal (ignoring case) to the given one
     */
    public boolean equalsIgnoreCase(Index otherIndex);

    /**
     * Returns a verbose string representation of this index.
     * 
     * @return The string representation
     */
    public String toVerboseString();
}
