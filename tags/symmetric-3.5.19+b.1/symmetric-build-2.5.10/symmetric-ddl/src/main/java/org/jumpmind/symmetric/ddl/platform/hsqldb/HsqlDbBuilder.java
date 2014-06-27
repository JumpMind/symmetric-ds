package org.jumpmind.symmetric.ddl.platform.hsqldb;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.alteration.AddColumnChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveColumnChange;
import org.jumpmind.symmetric.ddl.alteration.TableChange;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.SqlBuilder;

/*
 * The SQL Builder for the HsqlDb database.
 * 
 * @version $Revision: 518485 $
 */
public class HsqlDbBuilder extends SqlBuilder
{
    /*
     * Creates a new builder instance.
     * 
     * @param platform The plaftform this builder belongs to
     */
    public HsqlDbBuilder(Platform platform)
    {
        super(platform);
        addEscapedCharSequence("'", "''");
    }

    /*
     * {@inheritDoc}
     */
    public void dropTable(Table table) throws IOException
    { 
        print("DROP TABLE ");
        printIdentifier(getTableName(table));
        print(" IF EXISTS");
        printEndOfStatement();
    }

    /*
     * {@inheritDoc}
     */
    public String getSelectLastIdentityValues(Table table) 
    {
        return "CALL IDENTITY()";
    }

    /*
     * {@inheritDoc}
     */
    protected void processTableStructureChanges(Database currentModel,
                                                Database desiredModel,
                                                Table    sourceTable,
                                                Table    targetTable,
                                                Map      parameters,
                                                List     changes) throws IOException
    {
        // HsqlDb can only drop columns that are not part of a primary key
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if ((change instanceof RemoveColumnChange) && 
                ((RemoveColumnChange)change).getColumn().isPrimaryKey())
            {
                return;
            }
        }

        // in order to utilize the ALTER TABLE ADD COLUMN BEFORE statement
        // we have to apply the add column changes in the correct order
        // thus we first gather all add column changes and then execute them
        // Since we get them in target table column order, we can simply
        // iterate backwards
        ArrayList addColumnChanges = new ArrayList();

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof AddColumnChange)
            {
                addColumnChanges.add(change);
                changeIt.remove();
            }
        }
        for (ListIterator changeIt = addColumnChanges.listIterator(addColumnChanges.size()); changeIt.hasPrevious();)
        {
            AddColumnChange addColumnChange = (AddColumnChange)changeIt.previous();

            processChange(currentModel, desiredModel, addColumnChange);
            changeIt.remove();
        }

        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof RemoveColumnChange) 
            {
                RemoveColumnChange removeColumnChange = (RemoveColumnChange)change;

                processChange(currentModel, desiredModel, removeColumnChange);
                changeIt.remove();
            }
        }
    }

    /*
     * Processes the addition of a column to a table.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database        currentModel,
                                 Database        desiredModel,
                                 AddColumnChange change) throws IOException
    {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn());
        if (change.getNextColumn() != null)
        {
            print(" BEFORE ");
            printIdentifier(getColumnName(change.getNextColumn()));
        }
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a column from a table.
     * 
     * @param currentModel The current database schema
     * @param desiredModel The desired database schema
     * @param change       The change object
     */
    protected void processChange(Database           currentModel,
                                 Database           desiredModel,
                                 RemoveColumnChange change) throws IOException
    {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }
}
