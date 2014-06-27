package org.jumpmind.db.platform.postgresql;

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

import java.sql.Types;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;

/*
 * The SQL Builder for PostgresSql.
 */
public class PostgreSqlDdlBuilder extends AbstractDdlBuilder {

    public PostgreSqlDdlBuilder() {
        // this is the default length though it might be changed when building
        // PostgreSQL
        // in file src/include/postgres_ext.h
        databaseInfo.setMaxIdentifierLength(31);

        databaseInfo.addNativeTypeMapping(Types.ARRAY, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.BINARY, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN");
        databaseInfo.addNativeTypeMapping(Types.BLOB, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
        databaseInfo.addNativeTypeMapping(Types.DECIMAL, "NUMERIC", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.DISTINCT, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        databaseInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        databaseInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.LONGVARBINARY, "BYTEA");
        databaseInfo.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.LONGVARCHAR);
        databaseInfo.addNativeTypeMapping(Types.NULL, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.OTHER, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.REF, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.STRUCT, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        databaseInfo.addNativeTypeMapping(Types.VARBINARY, "BYTEA", Types.LONGVARBINARY);
        databaseInfo.addNativeTypeMapping("BOOLEAN", "BOOLEAN", "BIT");
        databaseInfo.addNativeTypeMapping("DATALINK", "BYTEA", "LONGVARBINARY");

        databaseInfo.setDefaultSize(Types.CHAR, 254);
        databaseInfo.setDefaultSize(Types.VARCHAR, 254);

        // no support for specifying the size for these types (because they are
        // mapped to BYTEA which back-maps to BLOB)
        databaseInfo.setHasSize(Types.BINARY, false);
        databaseInfo.setHasSize(Types.VARBINARY, false);

        databaseInfo.setNonBlankCharColumnSpacePadded(true);
        databaseInfo.setBlankCharColumnSpacePadded(true);
        databaseInfo.setCharColumnSpaceTrimmed(false);
        databaseInfo.setEmptyStringNulled(false);
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
    
    public static boolean isUsePseudoSequence() {
        return "true".equalsIgnoreCase(System.getProperty("org.jumpmind.symmetric.ddl.use.table.seq", "false"));
    }    

    @Override
    public void dropTable(Table table, StringBuilder ddl) {
        ddl.append("DROP TABLE ");
        printIdentifier(getTableName(table.getName()), ddl);
        ddl.append(" CASCADE");
        printEndOfStatement(ddl);

        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++) {
            dropAutoIncrementSequence(table, columns[idx], ddl);
        }
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        ddl.append("DROP INDEX ");
        printIdentifier(getIndexName(index), ddl);
        printEndOfStatement(ddl);
    }

    @Override
    public void createTable(Table table, StringBuilder ddl) {
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (column.isAutoIncrement()) {
                createAutoIncrementSequence(table, column, ddl);
            }
        }
        super.createTable(table, ddl);
    }

    /*
     * Creates the auto-increment sequence that is then used in the column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    private void createAutoIncrementSequence(Table table, Column column, StringBuilder ddl) {
        if (isUsePseudoSequence()) {
            ddl.append("CREATE TABLE ");
            ddl.append(getConstraintName(null, table, column.getName(), "tbl"));
            ddl.append("(SEQ_ID int8)");
            printEndOfStatement(ddl);

            ddl.append("CREATE FUNCTION ");
            ddl.append(getConstraintName(null, table, column.getName(), "seq"));
            ddl.append("() ");
            ddl.append("RETURNS INT8 AS $$ ");
            ddl.append("DECLARE curVal int8; ");
            ddl.append("BEGIN ");
            ddl.append("  select seq_id into curVal from ");
            ddl.append(getConstraintName(null, table, column.getName(), "tbl"));
            ddl.append(" for update;");
            ddl.append("  if curVal is null then ");
            ddl.append("      insert into ");
            ddl.append(getConstraintName(null, table, column.getName(), "tbl"));
            ddl.append(" values(1); ");
            ddl.append("      curVal = 0; ");
            ddl.append("  else ");
            ddl.append("      update ");
            ddl.append(getConstraintName(null, table, column.getName(), "tbl"));
            ddl.append(" set seq_id=curVal+1; ");
            ddl.append("  end if; ");
            ddl.append("  return curVal+1; ");
            ddl.append("END; ");
            println("$$ LANGUAGE plpgsql; ", ddl);
        } else {
            ddl.append("CREATE SEQUENCE ");
            printIdentifier(getConstraintName(null, table, column.getName(), "seq"), ddl);
            printEndOfStatement(ddl);
        }
    }

    /*
     * Creates the auto-increment sequence that is then used in the column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    private void dropAutoIncrementSequence(Table table, Column column, StringBuilder ddl) {
        if (isUsePseudoSequence()) {
            ddl.append("DROP TABLE ");
            ddl.append(getConstraintName(null, table, column.getName(), "tbl"));
            printEndOfStatement(ddl);

            ddl.append("DROP FUNCTION ");
            ddl.append(getConstraintName(null, table, column.getName(), "seq"));
            ddl.append("()");
            printEndOfStatement(ddl);
        } else {
            ddl.append("DROP SEQUENCE ");
            printIdentifier(getConstraintName(null, table, column.getName(), "seq"), ddl);
            printEndOfStatement(ddl);
        }
    }

    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        if (isUsePseudoSequence()) {
            ddl.append(" DEFAULT ");
            ddl.append(getConstraintName(null, table, column.getName(), "seq"));
            ddl.append("()");
        } else {
            ddl.append(" DEFAULT nextval('");
            printIdentifier(getConstraintName(null, table, column.getName(), "seq"), ddl);
            ddl.append("')");
        }
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
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
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
                    processChange(currentModel, desiredModel, addColumnChange, ddl);
                    changeIt.remove();
                }
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change, ddl);
                changeIt.remove();
            }
        }
        super.processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable,
                changes, ddl);
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
            AddColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("ADD COLUMN ");
        writeColumn(change.getChangedTable(), change.getNewColumn(), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
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
            RemoveColumnChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()), ddl);
        printEndOfStatement(ddl);
        if (change.getColumn().isAutoIncrement()) {
            dropAutoIncrementSequence(change.getChangedTable(), change.getColumn(), ddl);
        }
        change.apply(currentModel, delimitedIdentifierModeOn);
    }
}
