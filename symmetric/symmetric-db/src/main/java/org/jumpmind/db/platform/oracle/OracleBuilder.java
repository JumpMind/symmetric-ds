package org.jumpmind.db.platform.oracle;

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
import java.sql.Types;
import java.util.Iterator;
import java.util.List;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.jumpmind.db.DdlUtilsException;
import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Index;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.SqlBuilder;
import org.jumpmind.db.util.Jdbc3Utils;
import org.jumpmind.util.Log;

/*
 * The SQL Builder for Oracle.
 */
public class OracleBuilder extends SqlBuilder {
    
    /* The regular expression pattern for ISO dates, i.e. 'YYYY-MM-DD'. */
    private Pattern isoDatePattern;

    /* The regular expression pattern for ISO times, i.e. 'HH:MI:SS'. */
    private Pattern isoTimePattern;

    /*
     * The regular expression pattern for ISO timestamps, i.e. 'YYYY-MM-DD
     * HH:MI:SS.fffffffff'.
     */
    private Pattern isoTimestampPattern;

    public OracleBuilder(Log log, IDatabasePlatform platform, Writer writer) {
        super(log, platform, writer);

        addEscapedCharSequence("'", "''");

        PatternCompiler compiler = new Perl5Compiler();

        try {
            isoDatePattern = compiler.compile("\\d{4}\\-\\d{2}\\-\\d{2}");
            isoTimePattern = compiler.compile("\\d{2}:\\d{2}:\\d{2}");
            isoTimestampPattern = compiler
                    .compile("\\d{4}\\-\\d{2}\\-\\d{2} \\d{2}:\\d{2}:\\d{2}[\\.\\d{1,8}]?");
        } catch (MalformedPatternException ex) {
            throw new DdlUtilsException(ex);
        }
    }

    @Override
    public void createTable(Database database, Table table)  {
        // lets create any sequences
        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++) {
            createAutoIncrementSequence(table, columns[idx]);
        }

        super.createTable(database, table);

        for (int idx = 0; idx < columns.length; idx++) {
            createAutoIncrementTrigger(table, columns[idx]);
        }
    }

    @Override
    public void dropTable(Table table)  {
        // The only difference to the Oracle 8/9 variant is the purge which
        // prevents the
        // table from being moved to the recycle bin (which is new in Oracle 10)
        Column[] columns = table.getAutoIncrementColumns();

        for (int idx = 0; idx < columns.length; idx++) {
            dropAutoIncrementTrigger(table, columns[idx]);
            dropAutoIncrementSequence(table, columns[idx]);
        }

        print("DROP TABLE ");
        printIdentifier(getTableName(table));
        print(" CASCADE CONSTRAINTS PURGE");
        printEndOfStatement();
    }

    /*
     * Creates the sequence necessary for the auto-increment of the given
     * column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    protected void createAutoIncrementSequence(Table table, Column column)  {
        print("CREATE SEQUENCE ");
        printIdentifier(getConstraintName("seq", table, column.getName(), null));
        printEndOfStatement();
    }

    /*
     * Creates the trigger necessary for the auto-increment of the given column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    protected void createAutoIncrementTrigger(Table table, Column column)  {
        String columnName = getColumnName(column);
        String triggerName = getConstraintName("trg", table, column.getName(), null);

        if (platform.isScriptModeOn()) {
            // For the script, we output a more nicely formatted version
            print("CREATE OR REPLACE TRIGGER ");
            printlnIdentifier(triggerName);
            print("BEFORE INSERT ON ");
            printlnIdentifier(getTableName(table));
            print("FOR EACH ROW WHEN (new.");
            printIdentifier(columnName);
            println(" IS NULL)");
            println("BEGIN");
            print("  SELECT ");
            printIdentifier(getConstraintName("seq", table, column.getName(), null));
            print(".nextval INTO :new.");
            printIdentifier(columnName);
            print(" FROM dual");
            println(platform.getPlatformInfo().getSqlCommandDelimiter());
            print("END");
            println(platform.getPlatformInfo().getSqlCommandDelimiter());
            println("/");
            println();
        } else {
            // note that the BEGIN ... SELECT ... END; is all in one line and
            // does
            // not contain a semicolon except for the END-one
            // this way, the tokenizer will not split the statement before the
            // END
            print("CREATE OR REPLACE TRIGGER ");
            printIdentifier(triggerName);
            print(" BEFORE INSERT ON ");
            printIdentifier(getTableName(table));
            print(" FOR EACH ROW WHEN (new.");
            printIdentifier(columnName);
            println(" IS NULL)");
            print("BEGIN SELECT ");
            printIdentifier(getConstraintName("seq", table, column.getName(), null));
            print(".nextval INTO :new.");
            printIdentifier(columnName);
            print(" FROM dual");
            print(platform.getPlatformInfo().getSqlCommandDelimiter());
            print(" END");
            // It is important that there is a semicolon at the end of the
            // statement (or more
            // precisely, at the end of the PL/SQL block), and thus we put two
            // semicolons here
            // because the tokenizer will remove the one at the end
            print(platform.getPlatformInfo().getSqlCommandDelimiter());
            printEndOfStatement();
        }
    }

    /*
     * Drops the sequence used for the auto-increment of the given column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    protected void dropAutoIncrementSequence(Table table, Column column)  {
        print("DROP SEQUENCE ");
        printIdentifier(getConstraintName("seq", table, column.getName(), null));
        printEndOfStatement();
    }

    /*
     * Drops the trigger used for the auto-increment of the given column.
     * 
     * @param table The table
     * 
     * @param column The column
     */
    protected void dropAutoIncrementTrigger(Table table, Column column)  {
        print("DROP TRIGGER ");
        printIdentifier(getConstraintName("trg", table, column.getName(), null));
        printEndOfStatement();
    }

    @Override
    protected void createTemporaryTable(Database database, Table table)  {
        createTable(database, table);
    }

    @Override
    protected void dropTemporaryTable(Database database, Table table)  {
        dropTable(table);
    }

    @Override
    public void dropExternalForeignKeys(Table table)  {
        // no need to as we drop the table with CASCASE CONSTRAINTS
    }

    @Override
    public void writeExternalIndexDropStmt(Table table, Index index)  {
        // Index names in Oracle are unique to a schema and hence Oracle does
        // not
        // use the ON <tablename> clause
        print("DROP INDEX ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }

    @Override
    protected void printDefaultValue(Object defaultValue, int typeCode)  {
        if (defaultValue != null) {
            String defaultValueStr = defaultValue.toString();
            boolean shouldUseQuotes = !TypeMap.isNumericType(typeCode)
                    && !defaultValueStr.startsWith("TO_DATE(");

            if (shouldUseQuotes) {
                // characters are only escaped when within a string literal
                print(platform.getPlatformInfo().getValueQuoteToken());
                print(escapeStringValue(defaultValueStr));
                print(platform.getPlatformInfo().getValueQuoteToken());
            } else {
                print(defaultValueStr);
            }
        }
    }

    @Override
    protected String getNativeDefaultValue(Column column) {
        if ((column.getTypeCode() == Types.BIT)
                || (Jdbc3Utils.supportsJava14JdbcTypes() && (column.getTypeCode() == Jdbc3Utils
                        .determineBooleanTypeCode()))) {
            return getDefaultValueHelper().convert(column.getDefaultValue(), column.getTypeCode(),
                    Types.SMALLINT).toString();
        }
        // Oracle does not accept ISO formats, so we have to convert an ISO spec
        // if we find one
        // But these are the only formats that we make sure work, every other
        // format has to be database-dependent
        // and thus the user has to ensure that it is correct
        else if (column.getTypeCode() == Types.DATE) {
            if (new Perl5Matcher().matches(column.getDefaultValue(), isoDatePattern)) {
                return "TO_DATE('" + column.getDefaultValue() + "', 'YYYY-MM-DD')";
            }
        } else if (column.getTypeCode() == Types.TIME) {
            if (new Perl5Matcher().matches(column.getDefaultValue(), isoTimePattern)) {
                return "TO_DATE('" + column.getDefaultValue() + "', 'HH24:MI:SS')";
            }
        } else if (column.getTypeCode() == Types.TIMESTAMP) {
            if (new Perl5Matcher().matches(column.getDefaultValue(), isoTimestampPattern)) {
                return "TO_DATE('" + column.getDefaultValue() + "', 'YYYY-MM-DD HH24:MI:SS')";
            }
        }
        return super.getNativeDefaultValue(column);
    }

    @Override
    protected void writeColumnAutoIncrementStmt(Table table, Column column)  {
        // we're using sequences instead
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        Column[] columns = table.getAutoIncrementColumns();

        if (columns.length > 0) {
            StringBuffer result = new StringBuffer();

            result.append("SELECT ");
            for (int idx = 0; idx < columns.length; idx++) {
                if (idx > 0) {
                    result.append(",");
                }
                result.append(getDelimitedIdentifier(getConstraintName("seq", table,
                        columns[idx].getName(), null)));
                result.append(".currval");
            }
            result.append(" FROM dual");
            return result.toString();
        } else {
            return null;
        }
    }

    @Override
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes)  {
        // While Oracle has an ALTER TABLE MODIFY statement, it is somewhat
        // limited
        // esp. if there is data in the table, so we don't use it
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;

                // Oracle can only add not insert columns
                // Also, we cannot add NOT NULL columns unless they have a
                // default value
                if (!addColumnChange.isAtEnd()
                        || (addColumnChange.getNewColumn().isRequired() && (addColumnChange
                                .getNewColumn().getDefaultValue() == null))) {
                    // we need to rebuild the full table
                    return;
                }
            }
        }

        // First we drop primary keys as necessary
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof RemovePrimaryKeyChange) {
                processChange(currentModel, desiredModel, (RemovePrimaryKeyChange) change);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                RemovePrimaryKeyChange removePkChange = new RemovePrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getOldPrimaryKeyColumns());

                processChange(currentModel, desiredModel, removePkChange);
            }
        }

        // Next we add/remove columns
        // While Oracle has an ALTER TABLE MODIFY statement, it is somewhat
        // limited
        // esp. if there is data in the table, so we don't use it
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                processChange(currentModel, desiredModel, (AddColumnChange) change);
                changeIt.remove();
            } else if (change instanceof RemoveColumnChange) {
                processChange(currentModel, desiredModel, (RemoveColumnChange) change);
                changeIt.remove();
            }
        }
        // Finally we add primary keys
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddPrimaryKeyChange) {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change);
                changeIt.remove();
            } else if (change instanceof PrimaryKeyChange) {
                PrimaryKeyChange pkChange = (PrimaryKeyChange) change;
                AddPrimaryKeyChange addPkChange = new AddPrimaryKeyChange(
                        pkChange.getChangedTable(), pkChange.getNewPrimaryKeyColumns());

                processChange(currentModel, desiredModel, addPkChange);
                changeIt.remove();
            }
        }
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
        print("ADD ");
        writeColumn(change.getChangedTable(), change.getNewColumn());
        printEndOfStatement();
        if (change.getNewColumn().isAutoIncrement()) {
            createAutoIncrementSequence(change.getChangedTable(), change.getNewColumn());
            createAutoIncrementTrigger(change.getChangedTable(), change.getNewColumn());
        }
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
        if (change.getColumn().isAutoIncrement()) {
            dropAutoIncrementTrigger(change.getChangedTable(), change.getColumn());
            dropAutoIncrementSequence(change.getChangedTable(), change.getColumn());
        }
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP COLUMN ");
        printIdentifier(getColumnName(change.getColumn()));
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    /*
     * Processes the removal of a primary key from a table.
     * 
     * @param currentModel The current database schema
     * 
     * @param desiredModel The desired database schema
     * 
     * @param change The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemovePrimaryKeyChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP PRIMARY KEY");
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

}
