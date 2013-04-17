package org.jumpmind.symmetric.ddl.alteration;

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

import org.jumpmind.symmetric.ddl.DdlUtilsException;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Represents the addition of an index to a table.
 * 
 * @version $Revision: $
 */
public class AddIndexChange extends TableChangeImplBase
{
    /** The new index. */
    private Index _newIndex;

    /**
     * Creates a new change object.
     * 
     * @param table    The table to add the index to
     * @param newIndex The new index
     */
    public AddIndexChange(Table table, Index newIndex)
    {
        super(table);
        _newIndex = newIndex;
    }

    /**
     * Returns the new index.
     *
     * @return The new index
     */
    public Index getNewIndex()
    {
        return _newIndex;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive)
    {
        Index newIndex = null;

        try
        {
            newIndex = (Index)_newIndex.clone();
        }
        catch (CloneNotSupportedException ex)
        {
            throw new DdlUtilsException(ex);
        }
        database.findTable(getChangedTable().getName(), caseSensitive).addIndex(newIndex);
    }
}
