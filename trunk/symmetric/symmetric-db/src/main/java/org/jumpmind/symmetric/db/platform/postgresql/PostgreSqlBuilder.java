package org.jumpmind.symmetric.db.platform.postgresql;

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

import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.symmetric.db.IDatabasePlatform;
import org.jumpmind.symmetric.db.alter.AddColumnChange;
import org.jumpmind.symmetric.db.alter.RemoveColumnChange;
import org.jumpmind.symmetric.db.alter.TableChange;
import org.jumpmind.symmetric.db.model.Column;
import org.jumpmind.symmetric.db.model.Database;
import org.jumpmind.symmetric.db.model.Index;
import org.jumpmind.symmetric.db.model.Table;
import org.jumpmind.symmetric.db.platform.SqlBuilder;
import org.jumpmind.symmetric.util.Log;

/*
 * The SQL Builder for PostgresSql.
 */
public class PostgreSqlBuilder extends SqlBuilder {
    
    public PostgreSqlBuilder(Log log, IDatabasePlatform platform, Writer writer) {
        super(log, platform, writer);

        // we need to handle the backslash first otherwise the other
        // already escaped sequences would be affected
        addEscapedCharSequence("\\", "\\\\");
        addEscapedCharSequence("'", "\\'");
        addEscapedCharSequence("\b", "\\b");
        addEscapedCharSequence("\f", "\\f");
        addEscapedCharSequence("\n", "\\n");
        addEscapedCharSequence("\r", "\\r");
        addEscapedCharSequence("\t", "\\t");
    }

    @Override
    public void dropTable(Table table)  {
        print("DROP TABLE ");
        printIdentifier(getTableName(table));
        print(" CASCADE");
        printEndOfStatement();

        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++) {
            dropAutoIncrementSequence(table, columns[idx]);
        }
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, Index index)  {
        print("DROP INDEX ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    @Override
    public void createTable(Database database, Table table)  {
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (column.isAutoIncrement()) {
                createAutoIncrementSequence(table, column);
            }
        }
        super.createTable(database, table);
    }

    /*
     * Creates the auto-increment sequence that is then used in the column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    private void createAutoIncrementSequence(Table table, Column column)  {
        print("CREATE SEQUENCE ");
        printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
        printEndOfStatement();
    }

    /*
     * Creates the auto-increment sequence that is then used in the column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    private void dropAutoIncrementSequence(Table table, Column column)  {
        print("DROP SEQUENCE ");
        printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
        printEndOfStatement();
    }

    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column)  {
        print(" DEFAULT nextval('");
        printIdentifier(getConstraintName(null, table, column.getName(), "seq"));
        print("')");
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        Column[] columns = table.getAutoIncrementColumns();

        if (columns.length == 0) {
            return null;
        } else {
            StringBuffer result = new StringBuffer();

            result.append("SELECT ");
            for (int idx = 0; idx < columns.length; idx++) {
                if (idx > 0) {
                    result.append(", ");
                }
                result.append("currval('");
                result.append(getDelimitedIdentifier(getConstraintName(null, table,
                        columns[idx].getName(), "seq")));
                result.append("') AS ");
                result.append(getDelimitedIdentifier(columns[idx].getName()));
            }
            return result.toString();
        }
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes)  {
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;

                // We can only use PostgreSQL-specific SQL if
                // * the column is not set to NOT NULL (the constraint would be
                // applied immediately
                // which will not work if there is already data in the table)
                // * the column has no default value (it would be applied after
                // the change which
                // means that PostgreSQL would behave differently from other
                // databases where the
                // default is applied to every column)
                // * the column is added at the end of the table (PostgreSQL
                // does not support
                // insertion of a column)
                if (!addColumnChange.getNewColumn().isRequired()
                        && (addColumnChange.getNewColumn().getDefaultValue() == null)
                        && (addColumnChange.getNextColumn() == null)) {
                    processChange(currentModel, desiredModel, addColumnChange);
                    changeIt.remove();
                }
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change);
                changeIt.remove();
            }
        }
        super.processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable,
                changes);
    }

    /*
     * Processes the addition of a column to a table.
     * 
     * @param currentModel The current database schema
     * 
     * @param desiredModel The desired database schema
     * 
     * @param change The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddColumnChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn());
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a column from a table.
     * 
     * @param currentModel The current database schema
     * 
     * @param desiredModel The desired database schema
     * 
     * @param change The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveColumnChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        if (change.getColumn().isAutoIncrement()) {
            dropAutoIncrementSequence(change.getChangedTable(), change.getColumn());
        }
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }
}
