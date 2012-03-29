package org.jumpmind.symmetric.core.db.alter;

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

import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.ForeignKey;
import org.jumpmind.symmetric.core.model.Table;

/**
 * Represents the addition of a foreign key to a table. Note that for simplicity
 * and because it fits the model, this change actually implements table change
 * for the table that the new foreign key will originate.
 * 
 * @version $Revision: $
 */
public class AddForeignKeyChange extends TableChangeImplBase {
    /** The new foreign key. */
    private ForeignKey _newForeignKey;

    /**
     * Creates a new change object.
     * 
     * @param table
     *            The table to add the foreign key to
     * @param newForeignKey
     *            The new foreign key
     */
    public AddForeignKeyChange(Table table, ForeignKey newForeignKey) {
        super(table);
        _newForeignKey = newForeignKey;
    }

    /**
     * Returns the new foreign key.
     * 
     * @return The new foreign key
     */
    public ForeignKey getNewForeignKey() {
        return _newForeignKey;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive) {
        ForeignKey newFK = null;

        try {
            newFK = (ForeignKey) _newForeignKey.clone();
            newFK.setForeignTable(database.findTable(_newForeignKey.getForeignTableName(),
                    caseSensitive));
        } catch (CloneNotSupportedException ex) {
            throw new SqlException(ex);
        }
        database.findTable(getChangedTable().getTableName()).addForeignKey(newFK);
    }

}
