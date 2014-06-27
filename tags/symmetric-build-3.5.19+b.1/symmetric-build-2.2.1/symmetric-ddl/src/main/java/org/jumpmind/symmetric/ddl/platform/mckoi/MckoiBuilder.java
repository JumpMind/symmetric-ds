package org.jumpmind.symmetric.ddl.platform.mckoi;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.alteration.AddColumnChange;
import org.jumpmind.symmetric.ddl.alteration.ColumnAutoIncrementChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveColumnChange;
import org.jumpmind.symmetric.ddl.alteration.TableChange;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.SqlBuilder;

/**
 * The SQL Builder for the Mckoi database.
 * 
 * @version $Revision: 463757 $
 */
public class MckoiBuilder extends SqlBuilder
{
    /**
     * Creates a new builder instance.
     * 
     * @param platform The plaftform this builder belongs to
     */
    public MckoiBuilder(Platform platform)
    {
        super(platform);
        // we need to handle the backslash first otherwise the other
        // already escaped sequence would be affected
        addEscapedCharSequence("\\", "\\\\");
        addEscapedCharSequence("'",  "\\'");
    }

    /**
     * {@inheritDoc}
     */
    public void createTable(Database database, Table table, Map parameters) throws IOException
    {
        // we use sequences instead of the UNIQUEKEY function because this way
        // we can read their values back
        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++)
        {
            createAutoIncrementSequence(table, columns[idx]);
        }

        super.createTable(database, table, parameters);
    }

    /**
     * {@inheritDoc}
     */
    public void dropTable(Table table) throws IOException
    { 
        print("DROP TABLE IF EXISTS ");
        printIdentifier(getTableName(table));
        printEndOfStatement();

        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++)
        {
            dropAutoIncrementSequence(table, columns[idx]);
        }
    }

    /**
     * Creates the sequence necessary for the auto-increment of the given column.
     * 
     * @param table  The table
     * @param column The column
     */
    protected void createAutoIncrementSequence(Table  table,
                                               Column column) throws IOException
    {
        print("CREATE SEQUENCE ");
        printIdentifier(getConstraintName("seq",
                                          table,
                                          column.getName(),
                                          null));
        printEndOfStatement();
    }

    /**
     * Drops the sequence used for the auto-increment of the given column.
     * 
     * @param table  The table
     * @param column The column
     */
    protected void dropAutoIncrementSequence(Table  table,
                                             Column column) throws IOException
    {
        print("DROP SEQUENCE ");
        printIdentifier(getConstraintName("seq",
                                          table,
                                          column.getName(),
                                          null));
        printEndOfStatement();
    }

    /**
     * {@inheritDoc}
     */
    protected void writeColumnDefaultValue(Table table, Column column) throws IOException
    {
        if (column.isAutoIncrement())
        {
            // we start at value 1 to avoid issues with jdbc
            print("NEXTVAL('");
            print(getConstraintName("seq", table, column.getName(), null));
            print("')");
        }
        else
        {
            super.writeColumnDefaultValue(table, column);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String getSelectLastIdentityValues(Table table)
    {
        Column[] columns = table.getAutoIncrementColumns();

        if (columns.length > 0)
        {
            StringBuffer result = new StringBuffer();

            result.append("SELECT ");
            for (int idx = 0; idx < columns.length; idx++)
            {
                if (idx > 0)
                {
                    result.append(",");
                }
                result.append("CURRVAL('");
                result.append(getConstraintName("seq", table, columns[idx].getName(), null));
                result.append("')");
            }
            return result.toString();
        }
        else
        {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void processTableStructureChanges(Database currentModel,
                                                Database desiredModel,
                                                Table    sourceTable,
                                                Table    targetTable,
                                                Map      parameters,
                                                List     changes) throws IOException
    {
        // McKoi has this nice ALTER CREATE TABLE statement which saves us a lot of work
        // We only have to handle auto-increment changes manually
        for (Iterator it = changes.iterator(); it.hasNext();)
        {
            TableChange change = (TableChange)it.next();

            if (change instanceof ColumnAutoIncrementChange)
            {
                Column column = ((ColumnAutoIncrementChange)change).getColumn();

                // we have to defer removal of the sequences until they are no longer used
                if (!column.isAutoIncrement())
                {
                    ColumnAutoIncrementChange autoIncrChange = (ColumnAutoIncrementChange)change;

                    createAutoIncrementSequence(autoIncrChange.getChangedTable(),
                                                autoIncrChange.getColumn());
                }
            }
            else if (change instanceof AddColumnChange)
            {
                AddColumnChange addColumnChange = (AddColumnChange)change;

                if (addColumnChange.getNewColumn().isAutoIncrement())
                {
                    createAutoIncrementSequence(addColumnChange.getChangedTable(),
                                                addColumnChange.getNewColumn());
                }
            }
        }

        print("ALTER ");
        super.createTable(desiredModel, targetTable, parameters);

        for (Iterator it = changes.iterator(); it.hasNext();)
        {
            TableChange change = (TableChange)it.next();
    
            if (change instanceof ColumnAutoIncrementChange)
            {
                Column column = ((ColumnAutoIncrementChange)change).getColumn();
    
                if (column.isAutoIncrement())
                {
                    ColumnAutoIncrementChange autoIncrChange = (ColumnAutoIncrementChange)change;
        
                    dropAutoIncrementSequence(autoIncrChange.getChangedTable(),
                                              autoIncrChange.getColumn());
                }
            }
            else if (change instanceof RemoveColumnChange)
            {
                RemoveColumnChange removeColumnChange = (RemoveColumnChange)change;

                if (removeColumnChange.getColumn().isAutoIncrement())
                {
                    dropAutoIncrementSequence(removeColumnChange.getChangedTable(),
                                              removeColumnChange.getColumn());
                }
            }
        }
        changes.clear();
    }
}

