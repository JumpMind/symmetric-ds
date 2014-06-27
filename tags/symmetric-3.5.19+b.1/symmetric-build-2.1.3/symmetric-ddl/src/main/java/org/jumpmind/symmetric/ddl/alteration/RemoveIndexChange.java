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

import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.Table;

/**
 * Represents the removal of an index from a table.
 * 
 * @version $Revision: $
 */
public class RemoveIndexChange extends TableChangeImplBase
{
    /** The index to be removed. */
    private Index _index;

    /**
     * Creates a new change object.
     * 
     * @param table The table to remove the index from
     * @param index The index
     */
    public RemoveIndexChange(Table table, Index index)
    {
        super(table);
        _index = index;
    }

    /**
     * Returns the index.
     *
     * @return The index
     */
    public Index getIndex()
    {
        return _index;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive)
    {
        Table table = database.findTable(getChangedTable().getName(), caseSensitive);
        Index index = table.findIndex(_index.getName(), caseSensitive);

        table.removeIndex(index);
    }
}
