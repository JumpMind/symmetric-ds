package org.jumpmind.db.platform.hsqldb;

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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.IDatabasePlatform;

/*
 * The SQL Builder for the HsqlDb database.
 */
public class HsqlDbBuilder extends AbstractDdlBuilder {

    public HsqlDbBuilder(IDatabasePlatform platform) {
        super(platform);
        addEscapedCharSequence("'", "''");
    }

    @Override
    public void dropTable(Table table, StringBuilder ddl) {
        ddl.append("DROP TABLE ");
        printIdentifier(getTableName(table.getName()), ddl);
        ddl.append(" IF EXISTS");
        printEndOfStatement(ddl);
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        return "CALL IDENTITY()";
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        // HsqlDb can only drop columns that are not part of a primary key
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if ((change instanceof RemoveColumnChange)
                    && ((RemoveColumnChange) change).getColumn().isPrimaryKey()) {
                return;
            }
        }

        // in order to utilize the ALTER TABLE ADD COLUMN BEFORE statement
        // we have to apply the add column changes in the correct order
        // thus we first gather all add column changes and then execute them
        // Since we get them in target table column order, we can simply
        // iterate backwards
        ArrayList<AddColumnChange> addColumnChanges = new ArrayList<AddColumnChange>();

        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                addColumnChanges.add((AddColumnChange) change);
                changeIt.remove();
            }
        }

        for (ListIterator<AddColumnChange> changeIt = addColumnChanges
                .listIterator(addColumnChanges.size()); changeIt.hasPrevious();) {
            AddColumnChange addColumnChange = (AddColumnChange) changeIt.previous();

            processChange(currentModel, desiredModel, addColumnChange, ddl);
            changeIt.remove();
        }

        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof RemoveColumnChange) {
                RemoveColumnChange removeColumnChange = (RemoveColumnChange) change;

                processChange(currentModel, desiredModel, removeColumnChange, ddl);
                changeIt.remove();
            }
        }
    }

    /*
     * Processes the addition of a column to a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        if (change.getNextColumn() != null) {
            ddl.append(" BEFORE ");
            printIdentifier(getColumnName(change.getNextColumn()), ddl);
        }
        printEndOfStatement(ddl);
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a column from a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

}
