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

import java.util.ArrayList;
import java.util.Map;

import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Table;

/**
 * Represents the change of the order of the columns of a table.
 * 
 * @version $Revision: $
 */
public class ColumnOrderChange extends TableChangeImplBase {
    /** The map containing the new positions keyed by the source columns. */
    private Map<Column, Integer> _newPositions;

    /**
     * Creates a new change object.
     * 
     * @param table
     *            The table whose primary key is to be changed
     * @param newPositions
     *            The map containing the new positions keyed by the source
     *            columns
     */
    public ColumnOrderChange(Table table, Map<Column, Integer> newPositions) {
        super(table);
        _newPositions = newPositions;
    }

    /**
     * Returns the new position of the given source column.
     * 
     * @param sourceColumn
     *            The column
     * @return The new position or -1 if no position is marked for the column
     */
    public int getNewPosition(Column sourceColumn) {
        Integer newPos = (Integer) _newPositions.get(sourceColumn);

        return newPos == null ? -1 : newPos.intValue();
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database database, boolean caseSensitive) {
        Table table = database.findTable(getChangedTable().getTableName(), caseSensitive);
        ArrayList<Column> newColumns = new ArrayList<Column>(table.getColumnCount());

        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);
            int newPos = getNewPosition(column);

            newColumns.set(newPos < 0 ? idx : newPos, column);
        }
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            table.removeColumn(idx);
        }
        table.addColumns(newColumns);
    }
}
