package org.jumpmind.symmetric.ddl.platform.postgresql;

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
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.alteration.AddColumnChange;
import org.jumpmind.symmetric.ddl.alteration.RemoveColumnChange;
import org.jumpmind.symmetric.ddl.alteration.TableChange;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.SqlBuilder;

/*
 * The SQL Builder for PostgresSql.
 * 
 * @version $Revision: 504014 $
 */
public class PostgreSqlBuilder extends SqlBuilder
{
    /*
     * Creates a new builder instance.
     * 
     * @param platform The plaftform this builder belongs to
     */
    public PostgreSqlBuilder(Platform platform)
    {
        super(platform);
        // we need to handle the backslash first otherwise the other
        // already escaped sequences would be affected
        addEscapedCharSequence("\\", "\\\\");
        addEscapedCharSequence("'",  "\\'");
        addEscapedCharSequence("\b", "\\b");
        addEscapedCharSequence("\f", "\\f");
        addEscapedCharSequence("\n", "\\n");
        addEscapedCharSequence("\r", "\\r");
        addEscapedCharSequence("\t", "\\t");
    }

    /*
     * {@inheritDoc}
     */
    public void dropTable(Table table) throws IOException
    { 
        print("DROP TABLE ");
        printIdentifier(getTableName(table));
        print(" CASCADE");
        printEndOfStatement();

        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++)
        {
            dropAutoIncrementSequence(table, columns[idx]);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void writeExternalIndexDropStmt(Table table, Index index) throws IOException
    {
        print("DROP INDEX ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    /*
     * {@inheritDoc}
     */
    public void createTable(Database database, Table table, Map parameters) throws IOException
    {
        for (int idx = 0; idx < table.getColumnCount(); idx++)
        {
            Column column = table.getColumn(idx);

            if (column.isAutoIncrement())
            {
                createAutoIncrementSequence(table, column);
            }
        }
        super.createTable(database, table, parameters);
    }

    /*
     * Creates the auto-increment sequence that is then used in the column.
     *  
     * @param table  The table
     * @param column The column
     */
    private void createAutoIncrementSequence(Table table, Column column) throws IOException
    {
        if (PostgreSqlPlatform.isUsePseudoSequence()) {
            print("CREATE TABLE ");
            print(getConstraintName(null, table, column.getName(), "tbl"));
            print("(SEQ_ID int8)");
            printEndOfStatement();
            
            print("CREATE FUNCTION ");
            print(getConstraintName(null, table, column.getName(), "seq"));
            print("() ");
            print("RETURNS INT8 AS $$ ");
            print("DECLARE curVal int8; ");
            print("BEGIN ");
            print("  select seq_id into curVal from ");
            print(getConstraintName(null, table, column.getName(), "tbl"));
            print(" for update;");            
            print("  if curVal is null then ");
            print("      insert into ");
            print(getConstraintName(null, table, column.getName(), "tbl"));
            print(" values(1); ");
            print("      curVal = 0; ");
            print("  else "); 
            print("      update ");
            print(getConstraintName(null, table, column.getName(), "tbl"));
            print(" set seq_id=curVal+1; ");
            print("  end if; ");
            print("  return curVal+1; ");
            print("END; ");
            println("$$ LANGUAGE plpgsql; ");

        } else {
            print("CREATE SEQUENCE ");
            printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
            printEndOfStatement();
        }
    }

    /*
     * Creates the auto-increment sequence that is then used in the column.
     *  
     * @param table  The table
     * @param column The column
     */
    private void dropAutoIncrementSequence(Table table, Column column) throws IOException
    {
        if (PostgreSqlPlatform.isUsePseudoSequence()) {
            print("DROP TABLE ");
            print(getConstraintName(null, table, column.getName(), "tbl"));
            printEndOfStatement();
            
            print("DROP FUNCTION ");
            print(getConstraintName(null, table, column.getName(), "seq"));
            print("()");
            printEndOfStatement();
        } else {
            print("DROP SEQUENCE ");
            printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
            printEndOfStatement();
        }
    }

    /*
     * {@inheritDoc}
     */
    protected void writeColumnAutoIncrementStmt(Table table, Column column) throws IOException
    {
        if (PostgreSqlPlatform.isUsePseudoSequence()) {
            print(" DEFAULT ");
            print(getConstraintName(null, table, column.getName(), "seq"));
            print("()");
        } else {
            print(" DEFAULT nextval('");
            printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
            print("')");
        }
    }

    /*
     * {@inheritDoc}
     */
    public String getSelectLastIdentityValues(Table table)
    {
        Column[] columns = table.getAutoIncrementColumns();

        if (columns.length == 0)
        {
            return null;
        }
        else
        {
            StringBuffer result = new StringBuffer();
    
            result.append("SELECT ");
            for (int idx = 0; idx < columns.length; idx++)
            {
                if (idx > 0)
                {
                    result.append(", ");
                }
                result.append("currval('");
                result.append(getDelimitedIdentifier(getConstraintName(null, table, columns[idx].getName(), "seq")));
                result.append("') AS ");
                result.append(getDelimitedIdentifier(columns[idx].getName()));
            }
            return result.toString();
        }
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
        for (Iterator changeIt = changes.iterator(); changeIt.hasNext();)
        {
            TableChange change = (TableChange)changeIt.next();

            if (change instanceof AddColumnChange)
            {
                AddColumnChange addColumnChange = (AddColumnChange)change;

                // We can only use PostgreSQL-specific SQL if
                // * the column is not set to NOT NULL (the constraint would be applied immediately
                //   which will not work if there is already data in the table)
                // * the column has no default value (it would be applied after the change which
                //   means that PostgreSQL would behave differently from other databases where the
                //   default is applied to every column)
                // * the column is added at the end of the table (PostgreSQL does not support
                //   insertion of a column)
                if (!addColumnChange.getNewColumn().isRequired() &&
                    (addColumnChange.getNewColumn().getDefaultValue() == null) &&
                    (addColumnChange.getNextColumn() == null))
                {
                    processChange(currentModel, desiredModel, addColumnChange);
                    changeIt.remove();
                }
            }
            else if (change instanceof RemoveColumnChange)
            {
                processChange(currentModel, desiredModel, (RemoveColumnChange)change);
                changeIt.remove();
            }
        }
        super.processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable, parameters, changes);
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
        if (change.getColumn().isAutoIncrement())
        {
            dropAutoIncrementSequence(change.getChangedTable(), change.getColumn());
        }
        change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }
}
