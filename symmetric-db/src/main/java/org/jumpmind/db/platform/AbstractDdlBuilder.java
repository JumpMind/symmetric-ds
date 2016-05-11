package org.jumpmind.db.platform;

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
import java.text.DateFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.Closure;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.alter.AddColumnChange;
import org.jumpmind.db.alter.AddForeignKeyChange;
import org.jumpmind.db.alter.AddIndexChange;
import org.jumpmind.db.alter.AddPrimaryKeyChange;
import org.jumpmind.db.alter.AddTableChange;
import org.jumpmind.db.alter.ColumnAutoIncrementChange;
import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnDefaultValueChange;
import org.jumpmind.db.alter.ColumnRequiredChange;
import org.jumpmind.db.alter.ColumnSizeChange;
import org.jumpmind.db.alter.CopyColumnValueChange;
import org.jumpmind.db.alter.IModelChange;
import org.jumpmind.db.alter.ModelComparator;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemoveColumnChange;
import org.jumpmind.db.alter.RemoveForeignKeyChange;
import org.jumpmind.db.alter.RemoveIndexChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.alter.RemoveTableChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.ModelException;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.util.CallbackClosure;
import org.jumpmind.db.util.MultiInstanceofPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a collection of Strategy methods for creating the DDL required
 * to create and drop databases and tables.
 *
 * It is hoped that just a single implementation of this class, for each
 * database should make creating DDL for each physical database fairly
 * straightforward.
 *
 * An implementation of this class can always delegate down to some templating
 * technology such as Velocity if it requires. Though often that can be quite
 * complex when attempting to reuse code across many databases. Hopefully only a
 * small amount code needs to be changed on a per database basis.
 */
public abstract class AbstractDdlBuilder implements IDdlBuilder {

    /** The line separator for in between sql commands. */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

    /** The placeholder for the size value in the native type spec. */
    protected static final String SIZE_PLACEHOLDER = "{0}";

    /** The Log to which logging calls will be made. */
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /** The indentation used to indent commands. */
    private String indent = "    ";

    /** An optional locale specification for number and date formatting. */
    private String valueLocale;

    /** The date formatter. */
    private DateFormat valueDateFormat;

    /** The date time formatter. */
    private DateFormat valueTimeFormat;

    /** The number formatter. */
    private NumberFormat valueNumberFormat;

    /** Helper object for dealing with default values. */
    private DefaultValueHelper defaultValueHelper = new DefaultValueHelper();

    /** The character sequences that need escaping. */
    private Map<String, String> charSequencesToEscape = new LinkedHashMap<String, String>();

    protected DatabaseInfo databaseInfo = new DatabaseInfo();

    protected boolean delimitedIdentifierModeOn = true;
    
    protected boolean caseSensitive = true;

    protected boolean sqlCommentsOn = false;

    protected boolean scriptModeOn = false;
    
    protected String databaseName;

    /**
     * Creates a new sql builder.
     */
    public AbstractDdlBuilder(String databaseName) {
        this.databaseName = databaseName;
        addEscapedCharSequence("'", "''");
    }

    /**
     * Returns the default value helper.
     *
     * @return The default value helper
     */
    public DefaultValueHelper getDefaultValueHelper() {
        return defaultValueHelper;
    }

    /**
     * Returns the string used to indent the SQL.
     *
     * @return The indentation string
     */
    public String getIndent() {
        return indent;
    }

    /**
     * Sets the string used to indent the SQL.
     *
     * @param indent
     *            The indentation string
     */
    public void setIndent(String indent) {
        this.indent = indent;
    }

    /**
     * Returns the locale that is used for number and date formatting (when
     * printing default values and in generates insert/update/delete
     * statements).
     *
     * @return The locale or <code>null</code> if default formatting is used
     */
    public String getValueLocale() {
        return valueLocale;
    }

    /**
     * Sets the locale that is used for number and date formatting (when
     * printing default values and in generates insert/update/delete
     * statements).
     *
     * @param localeStr
     *            The new locale or <code>null</code> if default formatting
     *            should be used; Format is "language[_country[_variant]]"
     */
    public void setValueLocale(String localeStr) {
        if (localeStr != null) {
            int sepPos = localeStr.indexOf('_');
            String language = null;
            String country = null;
            String variant = null;

            if (sepPos > 0) {
                language = localeStr.substring(0, sepPos);
                country = localeStr.substring(sepPos + 1);
                sepPos = country.indexOf('_');
                if (sepPos > 0) {
                    variant = country.substring(sepPos + 1);
                    country = country.substring(0, sepPos);
                }
            } else {
                language = localeStr;
            }
            if (language != null) {
                Locale locale = null;

                if (variant != null) {
                    locale = new Locale(language, country, variant);
                } else if (country != null) {
                    locale = new Locale(language, country);
                } else {
                    locale = new Locale(language);
                }

                valueLocale = localeStr;
                setValueDateFormat(DateFormat.getDateInstance(DateFormat.SHORT, locale));
                setValueTimeFormat(DateFormat.getTimeInstance(DateFormat.SHORT, locale));
                setValueNumberFormat(NumberFormat.getNumberInstance(locale));
                return;
            }
        }
        valueLocale = null;
        setValueDateFormat(null);
        setValueTimeFormat(null);
        setValueNumberFormat(null);
    }

    /**
     * Returns the format object for formatting dates in the specified locale.
     *
     * @return The date format object or null if no locale is set
     */
    protected DateFormat getValueDateFormat() {
        return valueDateFormat;
    }

    /**
     * Sets the format object for formatting dates in the specified locale.
     *
     * @param format
     *            The date format object
     */
    protected void setValueDateFormat(DateFormat format) {
        this.valueDateFormat = format;
    }

    /**
     * Returns the format object for formatting times in the specified locale.
     *
     * @return The time format object or null if no locale is set
     */
    protected DateFormat getValueTimeFormat() {
        return valueTimeFormat;
    }

    /**
     * Sets the date format object for formatting times in the specified locale.
     *
     * @param format
     *            The time format object
     */
    protected void setValueTimeFormat(DateFormat format) {
        this.valueTimeFormat = format;
    }

    /**
     * Returns the format object for formatting numbers in the specified locale.
     *
     * @return The number format object or null if no locale is set
     */
    protected NumberFormat getValueNumberFormat() {
        return valueNumberFormat;
    }

    /**
     * Returns a new date format object for formatting numbers in the specified
     * locale. Platforms can override this if necessary.
     *
     * @param format
     *            The number format object
     */
    protected void setValueNumberFormat(NumberFormat format) {
        this.valueNumberFormat = format;
    }

    /**
     * Adds a char sequence that needs escaping, and its escaped version.
     *
     * @param charSequence
     *            The char sequence
     * @param escapedVersion
     *            The escaped version
     */
    protected void addEscapedCharSequence(String charSequence, String escapedVersion) {
        charSequencesToEscape.put(charSequence, escapedVersion);
    }

    public String createTables(Database database, boolean dropTables) {
        StringBuilder ddl = new StringBuilder();
        createTables(database, dropTables, ddl);
        return ddl.toString();
    }

    /**
     * Outputs the DDL required to drop (if requested) and (re)create all tables
     * in the database model.
     */
    public void createTables(Database database, boolean dropTables, StringBuilder ddl) {
        if (dropTables) {
            dropTables(database, ddl);
        }

        for (int idx = 0; idx < database.getTableCount(); idx++) {
            Table table = database.getTable(idx);

            writeTableComment(table, ddl);
            createTable(table, ddl, false, false);
        }

        // we're writing the external foreignkeys last to ensure that all
        // referenced tables are already defined
        createExternalForeignKeys(database, ddl);
    }

    public String alterDatabase(Database currentModel, Database desiredModel, IAlterDatabaseInterceptor... alterDatabaseInterceptors) {
        StringBuilder ddl = new StringBuilder();
        alterDatabase(currentModel, desiredModel, ddl, alterDatabaseInterceptors);
        return ddl.toString();
    }

    public String alterTable(Table currentTable, Table desiredTable, IAlterDatabaseInterceptor... alterDatabaseInterceptors) {        
        Database currentModel = new Database();
        currentModel.addTable(currentTable);

        Database desiredModel = new Database();
        desiredModel.addTable(desiredTable);

        return alterDatabase(currentModel, desiredModel, alterDatabaseInterceptors);
    }
    
    /**
     * When the platform columns were added alters were not taken into
     * consideration. Therefore, we copy the desiredPlatformColumn to the
     * current platform column because the alter code ends up using the current
     * column to come up with the alter statements. This is a hack that depends
     * on the fact that none of the alter decision are based upon the current
     * column's platform column.
     */
    protected void mergeOrRemovePlatformTypes(Database currentModel, Database desiredModel) {
        Table[] currentTables = currentModel.getTables();

        for (Table currentTable : currentTables) {
            /*
             * Warning - we don't currently support altering tables across
             * different catalogs and schemas, so we only need to look up by
             * name
             */
            Table desiredTable = desiredModel.findTable(currentTable.getName(), false);
            if (desiredTable != null) {
                Column[] currentColumns = currentTable.getColumns();
                for (Column currentColumn : currentColumns) {
                    Column desiredColumn = desiredTable.getColumnWithName(currentColumn.getName());
                    if (desiredColumn != null) {
                        currentColumn.removePlatformColumn(databaseName);
                        PlatformColumn desiredPlatformColumn = desiredColumn
                                .findPlatformColumn(databaseName);
                        if (desiredPlatformColumn != null) {
                            currentColumn.addPlatformColumn(desiredPlatformColumn);
                        }

                    }
                }
            }
        }
    }

    /**
     * Generates the DDL to modify an existing database so the schema matches
     * the specified database schema by using drops, modifications and
     * additions. Database-specific implementations can change aspect of this
     * algorithm by redefining the individual methods that compromise it.
     */
    public void alterDatabase(Database currentModel, Database desiredModel, StringBuilder ddl, IAlterDatabaseInterceptor... alterDatabaseInterceptors) {
        
        currentModel = currentModel.copy();
        mergeOrRemovePlatformTypes(currentModel, desiredModel);

        ModelComparator comparator = new ModelComparator(databaseName, databaseInfo, caseSensitive);
        List<IModelChange> detectedChanges = comparator.compare(currentModel, desiredModel);
        if (alterDatabaseInterceptors != null) {
            for (IAlterDatabaseInterceptor interceptor : alterDatabaseInterceptors) {
                detectedChanges = interceptor.intercept(detectedChanges, currentModel, desiredModel);
            }
        }
        processChanges(currentModel, desiredModel, detectedChanges, ddl);
    }

    public boolean isAlterDatabase(Database currentModel, Database desiredModel, IAlterDatabaseInterceptor... alterDatabaseInterceptors) {
        ModelComparator comparator = new ModelComparator(databaseName, databaseInfo, caseSensitive);
        List<IModelChange> detectedChanges = comparator.compare(currentModel, desiredModel);
        if (alterDatabaseInterceptors != null) {
            for (IAlterDatabaseInterceptor interceptor : alterDatabaseInterceptors) {
                detectedChanges = interceptor.intercept(detectedChanges, currentModel, desiredModel);
            }
        }
        return detectedChanges.size() > 0;
    }

    /**
     * Calls the given closure for all changes that are of one of the given
     * types, and then removes them from the changes collection.
     *
     * @param changes
     *            The changes
     * @param changeTypes
     *            The types to search for
     * @param closure
     *            The closure to invoke
     */
    protected void applyForSelectedChanges(Collection<IModelChange> changes,
            Class<?>[] changeTypes, final Closure closure) {
        final Predicate predicate = new MultiInstanceofPredicate(changeTypes);

        // basically we filter the changes for all objects where the above
        // predicate returns true, and for these filtered objects we invoke the
        // given closure
        CollectionUtils.filter(changes, new Predicate() {
            public boolean evaluate(Object obj) {
                if (predicate.evaluate(obj)) {
                    closure.execute(obj);
                    return false;
                } else {
                    return true;
                }
            }
        });
    }

    /**
     * Processes the changes. The default argument performs several passes:
     * <ol>
     * <li>
     * {@link org.jumpmind.db.alter.RemoveForeignKeyChange} and
     * {@link org.jumpmind.db.alter.RemoveIndexChange} come first to allow for
     * e.g. subsequent primary key changes or column removal.</li>
     * <li>{@link org.jumpmind.db.alter.RemoveTableChange} comes after the
     * removal of foreign keys and indices.</li>
     * <li>These are all handled together:<br/>
     * {@link org.jumpmind.db.alter.RemovePrimaryKeyChange}<br/>
     * {@link org.jumpmind.db.alter.AddPrimaryKeyChange}<br/>
     * {@link org.jumpmind.db.alter.PrimaryKeyChange}<br/>
     * {@link org.jumpmind.db.alter.RemoveColumnChange}<br/>
     * {@link org.jumpmind.db.alter.AddColumnChange}<br/>
     * {@link org.jumpmind.db.alter.ColumnAutoIncrementChange} <br/>
     * {@link org.jumpmind.db.alter.ColumnDefaultValueChange} <br/>
     * {@link org.jumpmind.db.alter.ColumnRequiredChange}<br/>
     * {@link org.jumpmind.db.alter.ColumnDataTypeChange}<br/>
     * {@link org.jumpmind.db.alter.ColumnSizeChange}<br/>
     * The reason for this is that the default algorithm rebuilds the table for
     * these changes and thus their order is irrelevant.</li>
     * <li>{@link org.jumpmind.db.alter.AddTableChange}<br/>
     * needs to come after the table removal (so that tables of the same name
     * are removed) and before the addition of foreign keys etc.</li>
     * <li>{@link org.jumpmind.db.alter.AddForeignKeyChange} and
     * {@link org.jumpmind.db.alter.AddIndexChange} come last after
     * table/column/primary key additions or changes.</li>
     * </ol>
     */
    @SuppressWarnings("unchecked")
    protected void processChanges(Database currentModel, Database desiredModel,
            List<IModelChange> changes, StringBuilder ddl) {
        CallbackClosure callbackClosure = new CallbackClosure(this, "processChange", new Class[] {
                Database.class, Database.class, null, StringBuilder.class }, new Object[] {
                currentModel, desiredModel, null, ddl });

        // 1st pass: removing external constraints and indices
        applyForSelectedChanges(changes, new Class[] { RemoveForeignKeyChange.class,
                RemoveIndexChange.class }, callbackClosure);

        // 2nd pass: removing tables
        applyForSelectedChanges(changes, new Class[] { RemoveTableChange.class }, callbackClosure);

        // 3rd pass: changing the structure of tables
        Predicate predicate = new MultiInstanceofPredicate(new Class[] {
                RemovePrimaryKeyChange.class, AddPrimaryKeyChange.class, PrimaryKeyChange.class,
                RemoveColumnChange.class, AddColumnChange.class, ColumnAutoIncrementChange.class,
                ColumnDefaultValueChange.class, ColumnRequiredChange.class,
                ColumnDataTypeChange.class, ColumnSizeChange.class, CopyColumnValueChange.class });

        processTableStructureChanges(currentModel, desiredModel,
                CollectionUtils.select(changes, predicate), ddl);

        // 4th pass: adding tables
        applyForSelectedChanges(changes, new Class[] { AddTableChange.class }, callbackClosure);
        // 5th pass: adding external constraints and indices
        applyForSelectedChanges(changes, new Class[] { AddForeignKeyChange.class,
                AddIndexChange.class }, callbackClosure);
    }

    /**
     * This is a fall-through callback which generates a warning because a
     * specific change type wasn't handled.
     */
    protected void processChange(Database currentModel, Database desiredModel, IModelChange change,
            StringBuilder ddl) {
        log.warn("Change of type " + change.getClass() + " was not handled");
    }

    /**
     * Processes the change representing the removal of a foreign key.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveForeignKeyChange change, StringBuilder ddl) {
        writeExternalForeignKeyDropStmt(change.getChangedTable(), change.getForeignKey(), ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /**
     * Processes the change representing the removal of an index.
     *
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param change
     *            The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveIndexChange change, StringBuilder ddl) {
        writeExternalIndexDropStmt(change.getChangedTable(), change.getIndex(), ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /**
     * Processes the change representing the removal of a table.
     *
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param change
     *            The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            RemoveTableChange change, StringBuilder ddl) {
        dropTable(change.getChangedTable(), ddl, false, false);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /**
     * Processes the change representing the addition of a table.
     *
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param change
     *            The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddTableChange change, StringBuilder ddl) {
        createTable(change.getNewTable(), ddl, false, false);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /**
     * Processes the change representing the addition of a foreign key.
     *
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param change
     *            The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddForeignKeyChange change, StringBuilder ddl) {
        writeExternalForeignKeyCreateStmt(desiredModel, change.getChangedTable(),
                change.getNewForeignKey(), ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /**
     * Processes the change representing the addition of an index.
     *
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param change
     *            The change object
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddIndexChange change, StringBuilder ddl) {
        writeExternalIndexCreateStmt(change.getChangedTable(), change.getNewIndex(), ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }
    
    protected void filterChanges(Collection<TableChange> changes) {
        
    }

    /**
     * Processes the changes to the structure of tables.
     *
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param changes
     *            The change objects
     */
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Collection<TableChange> changes, StringBuilder ddl) {
        
        filterChanges(changes);
        
        LinkedHashMap<String, List<TableChange>> changesPerTable = new LinkedHashMap<String, List<TableChange>>();
        LinkedHashMap<String, List<ForeignKey>> unchangedFKs = new LinkedHashMap<String, List<ForeignKey>>();
        boolean caseSensitive = delimitedIdentifierModeOn;

        // we first sort the changes for the tables
        // however since the changes might contain source or target tables
        // we use the names rather than the table objects
        for (Iterator<TableChange> changeIt = changes.iterator(); changeIt.hasNext();) {
            TableChange change = changeIt.next();
            String name = change.getChangedTable().getName();

            if (!caseSensitive) {
                name = name.toUpperCase();
            }

            List<TableChange> changesForTable = (List<TableChange>) changesPerTable.get(name);

            if (changesForTable == null) {
                changesForTable = new ArrayList<TableChange>();
                changesPerTable.put(name, changesForTable);
                unchangedFKs.put(name, getUnchangedForeignKeys(currentModel, desiredModel, name));
            }
            changesForTable.add(change);
        }
        // we also need to drop the foreign keys of the unchanged tables
        // referencing the changed tables
        addRelevantFKsFromUnchangedTables(currentModel, desiredModel, changesPerTable.keySet(),
                unchangedFKs);

        // we're dropping the unchanged foreign keys
        for (Iterator<Map.Entry<String, List<ForeignKey>>> tableFKIt = unchangedFKs.entrySet()
                .iterator(); tableFKIt.hasNext();) {
            Map.Entry<String, List<ForeignKey>> entry = tableFKIt.next();
            Table targetTable = desiredModel.findTable((String) entry.getKey(), caseSensitive);

            for (Iterator<ForeignKey> fkIt = entry.getValue().iterator(); fkIt.hasNext();) {
                writeExternalForeignKeyDropStmt(targetTable, fkIt.next(), ddl);
            }
        }

        // We're using a copy of the current model so that the table structure
        // changes can modify it
        Database copyOfCurrentModel = copy(currentModel);

        for (Iterator<Map.Entry<String, List<TableChange>>> tableChangeIt = changesPerTable
                .entrySet().iterator(); tableChangeIt.hasNext();) {
            Map.Entry<String, List<TableChange>> entry = tableChangeIt.next();
            processTableStructureChanges(copyOfCurrentModel, desiredModel, entry.getKey(),
                    entry.getValue(), ddl);
        }
        // and finally we're re-creating the unchanged foreign keys
        for (Iterator<Map.Entry<String, List<ForeignKey>>> tableFKIt = unchangedFKs.entrySet()
                .iterator(); tableFKIt.hasNext();) {
            Map.Entry<String, List<ForeignKey>> entry = tableFKIt.next();
            Table targetTable = desiredModel.findTable((String) entry.getKey(), caseSensitive);

            for (Iterator<ForeignKey> fkIt = entry.getValue().iterator(); fkIt.hasNext();) {
                writeExternalForeignKeyCreateStmt(desiredModel, targetTable, fkIt.next(), ddl);
            }
        }
    }

    /**
     * Determines the unchanged foreign keys of the indicated table.
     *
     * @param currentModel
     *            The current model
     * @param desiredModel
     *            The desired model
     * @param tableName
     *            The name of the table
     * @return The list of unchanged foreign keys
     */
    private List<ForeignKey> getUnchangedForeignKeys(Database currentModel, Database desiredModel,
            String tableName) {
        ArrayList<ForeignKey> unchangedFKs = new ArrayList<ForeignKey>();
        boolean caseSensitive = delimitedIdentifierModeOn;
        Table sourceTable = currentModel.findTable(tableName, caseSensitive);
        Table targetTable = desiredModel.findTable(tableName, caseSensitive);

        for (int idx = 0; idx < targetTable.getForeignKeyCount(); idx++) {
            ForeignKey targetFK = targetTable.getForeignKey(idx);
            ForeignKey sourceFK = sourceTable.findForeignKey(targetFK, caseSensitive);

            if (sourceFK != null) {
                unchangedFKs.add(targetFK);
            }
        }
        return unchangedFKs;
    }

    /**
     * Adds the foreign keys of the unchanged tables that reference changed
     * tables to the given map.
     *
     * @param currentModel
     *            The current model
     * @param desiredModel
     *            The desired model
     * @param namesOfKnownChangedTables
     *            The known names of changed tables
     * @param fksPerTable
     *            The map table name -> foreign keys to which found foreign keys
     *            will be added to
     */
    private void addRelevantFKsFromUnchangedTables(Database currentModel, Database desiredModel,
            Set<String> namesOfKnownChangedTables, Map<String, List<ForeignKey>> fksPerTable) {
        boolean caseSensitive = delimitedIdentifierModeOn;

        for (int tableIdx = 0; tableIdx < desiredModel.getTableCount(); tableIdx++) {
            Table targetTable = desiredModel.getTable(tableIdx);
            String name = targetTable.getName();
            Table sourceTable = currentModel.findTable(name, caseSensitive);
            List<ForeignKey> relevantFks = null;

            if (!caseSensitive) {
                name = name.toUpperCase();
            }
            if ((sourceTable != null) && !namesOfKnownChangedTables.contains(name)) {
                for (int fkIdx = 0; fkIdx < targetTable.getForeignKeyCount(); fkIdx++) {
                    ForeignKey targetFk = targetTable.getForeignKey(fkIdx);
                    ForeignKey sourceFk = sourceTable.findForeignKey(targetFk, caseSensitive);
                    String refName = targetFk.getForeignTableName();

                    if (!caseSensitive) {
                        refName = refName.toUpperCase();
                    }
                    if ((sourceFk != null) && namesOfKnownChangedTables.contains(refName)) {
                        if (relevantFks == null) {
                            relevantFks = new ArrayList<ForeignKey>();
                            fksPerTable.put(name, relevantFks);
                        }
                        relevantFks.add(targetFk);
                    }
                }
            }
        }
    }

    /**
     * Processes the changes to the structure of a single table.
     * Database-specific implementations might redefine this method, but it is
     * usually sufficient to redefine the
     * {@link #processTableStructureChanges(Database, Database, Table, Table, Map, List)}
     * method instead.
     */
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            String tableName, List<TableChange> changes, StringBuilder ddl) {

        StringBuilder tableDdl = new StringBuilder();

        Database originalModel = copy(currentModel);
        Table sourceTable = originalModel.findTable(tableName, delimitedIdentifierModeOn);
        Table targetTable = desiredModel.findTable(tableName, delimitedIdentifierModeOn);

        // we're enforcing a full rebuild in case of the addition of a required
        // column without a default value that is not autoincrement
        boolean requiresFullRebuild = false;

        for (Iterator<TableChange> changeIt = changes.iterator(); !requiresFullRebuild
                && changeIt.hasNext();) {
            TableChange change = changeIt.next();

            if (change instanceof AddColumnChange) {
                AddColumnChange addColumnChange = (AddColumnChange) change;

                if (addColumnChange.getNewColumn().isRequired()
                        && (addColumnChange.getNewColumn().getDefaultValue() == null)
                        && !addColumnChange.getNewColumn().isAutoIncrement()) {
                    requiresFullRebuild = true;
                }
            }
        }
        if (!requiresFullRebuild) {
            processTableStructureChanges(currentModel, desiredModel, sourceTable, targetTable,
                    changes, tableDdl);
        }

        if (!changes.isEmpty()) {
            // we can only copy the data if no required columns without default
            // value and non-autoincrement have been added
            boolean canMigrateData = true;

            for (Iterator<TableChange> it = changes.iterator(); canMigrateData && it.hasNext();) {
                TableChange change = it.next();

                if (change instanceof AddColumnChange) {
                    AddColumnChange addColumnChange = (AddColumnChange) change;

                    if (addColumnChange.getNewColumn().isRequired()
                            && !addColumnChange.getNewColumn().isAutoIncrement()
                            && (addColumnChange.getNewColumn().getDefaultValue() == null)) {
                        log.warn("Data cannot be retained in table "
                                + change.getChangedTable().getName()
                                + " because of the addition of the required column "
                                + addColumnChange.getNewColumn().getName());
                        canMigrateData = false;
                    }
                }
            }

            Table realTargetTable = getRealTargetTableFor(desiredModel, sourceTable, targetTable);

            if (canMigrateData) {
                Table tempTable = getTemporaryTableFor(sourceTable);
                dropTemporaryTable(tempTable, ddl);
                createTemporaryTable(tempTable, ddl);
                writeCopyDataStatement(sourceTable, tempTable, ddl);
                /*
                 * Note that we don't drop the indices here because the DROP
                 * TABLE will take care of that Likewise, foreign keys have
                 * already been dropped as necessary
                 */
                dropTable(sourceTable, ddl, false, true);
                createTable(realTargetTable, ddl, false, true);
                writeCopyDataStatement(tempTable, targetTable, ddl);
                dropTemporaryTable(tempTable, ddl);
                writeFixLastIdentityValues(targetTable, ddl);
            } else {
                dropTable(sourceTable, ddl, false, false);
                createTable(realTargetTable, ddl, false, false);
            }
        } else {
            ddl.append(tableDdl);
        }
    }
    
    protected Database copy(Database currentModel) {
        try {
            return (Database) currentModel.clone();
        } catch (CloneNotSupportedException ex) {
            throw new DdlException(ex);
        }
    }
    

    /**
     * Allows database-specific implementations to handle changes in a database
     * specific manner. Any handled change should be applied to the given
     * current model (which is a copy of the real original model) and be removed
     * from the list of changes.<br/>
     * In the default implementation, all {@link AddPrimaryKeyChange} changes
     * are applied via an <code>ALTER TABLE ADD CONSTRAINT</code> statement.
     *
     * @param currentModel
     *            The current database schema
     * @param desiredModel
     *            The desired database schema
     * @param sourceTable
     *            The original table
     * @param targetTable
     *            The desired table
     * @param changes
     *            The change objects for the target table
     */
    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Table sourceTable, Table targetTable, List<TableChange> changes, StringBuilder ddl) {
        if (changes.size() == 1) {
            TableChange change = changes.get(0);

            if (change instanceof AddPrimaryKeyChange) {
                processChange(currentModel, desiredModel, (AddPrimaryKeyChange) change, ddl);
                changes.clear();
            }
        }

        Iterator<TableChange> it = changes.iterator();
        while (it.hasNext()) {
            TableChange change = (TableChange) it.next();
            // Check to see if we can generate alters for type changes
            if (change instanceof ColumnDataTypeChange) {
                ColumnDataTypeChange typeChange = (ColumnDataTypeChange)change;
                if (typeChange.getNewTypeCode() == Types.BIGINT) {
                    if (writeAlterColumnDataTypeToBigInt(typeChange, ddl)) {
                        it.remove();
                    }
                }
            }
        }
    }
    
    protected void processChange(Database currentModel, Database desiredModel,
            CopyColumnValueChange change, StringBuilder ddl) {
        ddl.append("UPDATE ");
        ddl.append(getFullyQualifiedTableNameShorten(change.getChangedTable()));
        ddl.append(" SET ");
        printIdentifier(getColumnName(change.getTargetColumn()), ddl);
        ddl.append("=");
        printIdentifier(getColumnName(change.getSourceColumn()), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);

    }
    
    protected boolean writeAlterColumnDataTypeToBigInt(ColumnDataTypeChange change, StringBuilder ddl) {
        return false;
    }
    
    /**
	 * The fully qualified table name shorten
     */
    protected String getFullyQualifiedTableNameShorten(Table table) {
    	String result="";
        if (StringUtils.isNotBlank(table.getCatalog())) {
            result+=getDelimitedIdentifier(table.getCatalog()).concat(databaseInfo.getCatalogSeparator());
        }
        if (StringUtils.isNotBlank(table.getSchema())) {
        	result+=getDelimitedIdentifier(table.getSchema()).concat(databaseInfo.getSchemaSeparator());
        }
        result+=getDelimitedIdentifier(getTableName(table.getName()));
        return result;
    }
        
    /**
     * Creates a temporary table object that corresponds to the given table.
     * Database-specific implementations may redefine this method if e.g. the
     * database directly supports temporary tables. The default implementation
     * simply appends an underscore to the table name and uses that as the table
     * name.
     * @param targetTable
     *            The target table
     *
     * @return The temporary table
     */
    protected Table getTemporaryTableFor(Table targetTable) {
        return getTemporaryTableFor(targetTable, "_");
    }

    public Table getBackupTableFor(Table sourceTable) {
        return getTemporaryTableFor(sourceTable, "x");
    }

    public Table createBackupTableFor(Database model, Table sourceTable, StringBuilder ddl) {
        Table backupTable = getBackupTableFor(sourceTable);
        writeTableCreationStmt(backupTable, ddl);
        printEndOfStatement(ddl);
        writeCopyDataStatement(sourceTable, backupTable, ddl);
        return backupTable;
    }

    public void restoreTableFromBackup(Table backupTable, Table targetTable,
            LinkedHashMap<Column, Column> columnMap, StringBuilder ddl) {
        ddl.append("DELETE FROM ");
        ddl.append(getFullyQualifiedTableNameShorten(targetTable));
        printEndOfStatement(ddl);
        writeCopyDataStatement(backupTable, targetTable, columnMap, ddl);
    }

    protected Table getTemporaryTableFor(Table targetTable, String suffix) {
        Table table = new Table();

        table.setCatalog(targetTable.getCatalog());
        table.setSchema(targetTable.getSchema());
        table.setName(targetTable.getName() + suffix);
        table.setType(targetTable.getType());
        for (int idx = 0; idx < targetTable.getColumnCount(); idx++) {
            try {
                table.addColumn((Column) targetTable.getColumn(idx).clone());
            } catch (CloneNotSupportedException ex) {
                throw new DdlException(ex);
            }
        }

        return table;
    }

    /**
     * Outputs the DDL to create the given temporary table. Per default this is
     * simply a call to {@link #createTable(Table, Map)}.
     * @param table
     *            The table
     */
    protected void createTemporaryTable(Table table, StringBuilder ddl) {
        createTable(table, ddl, true, false);
    }

    /**
     * Outputs the DDL to drop the given temporary table. Per default this is
     * simply a call to {@link #dropTable(Table)}.
     * @param table
     *            The table
     */
    protected void dropTemporaryTable(Table table, StringBuilder ddl) {
        dropTable(table, ddl, true, false);
    }

    /**
     * Creates the target table object that differs from the given target table
     * only in the indices. More specifically, only those indices are used that
     * have not changed.
     *
     * @param targetModel
     *            The target database
     * @param sourceTable
     *            The source table
     * @param targetTable
     *            The target table
     * @return The table
     */
    protected Table getRealTargetTableFor(Database targetModel, Table sourceTable, Table targetTable) {
        Table table = new Table();

        table.setCatalog(targetTable.getCatalog());
        table.setSchema(targetTable.getSchema());
        table.setName(targetTable.getName());
        table.setType(targetTable.getType());
        for (int idx = 0; idx < targetTable.getColumnCount(); idx++) {
            try {
                table.addColumn((Column) targetTable.getColumn(idx).clone());
            } catch (CloneNotSupportedException ex) {
                throw new DdlException(ex);
            }
        }

        boolean caseSensitive = delimitedIdentifierModeOn;

        for (int idx = 0; idx < targetTable.getIndexCount(); idx++) {
            IIndex targetIndex = targetTable.getIndex(idx);
            IIndex sourceIndex = sourceTable.findIndex(targetIndex.getName(), caseSensitive);

            if (sourceIndex != null) {
                if ((caseSensitive && sourceIndex.equals(targetIndex))
                        || (!caseSensitive && sourceIndex.equalsIgnoreCase(targetIndex))) {
                    table.addIndex(targetIndex);
                }
            }
        }

        return table;
    }

    /**
     * Writes a statement that copies the data from the source to the target
     * table. Note that this copies only those columns that are in both tables.
     * Database-specific implementations might redefine this method though they
     * usually it suffices to redefine the
     * {@link #writeCastExpression(Column, Column)} method.
     *
     * @param sourceTable
     *            The source table
     * @param targetTable
     *            The target table
     */
    public void writeCopyDataStatement(Table sourceTable, Table targetTable, StringBuilder ddl) {
        LinkedHashMap<Column, Column> columnMap = getCopyDataColumnMapping(sourceTable, targetTable);
        writeCopyDataStatement(sourceTable, targetTable, columnMap, ddl);
    }

    public void writeCopyDataStatement(Table sourceTable, Table targetTable,
            LinkedHashMap<Column, Column> columnMap, StringBuilder ddl) {
        ddl.append("INSERT INTO ");
        ddl.append(getFullyQualifiedTableNameShorten(targetTable));
        ddl.append(" (");
        for (Iterator<Column> columnIt = columnMap.values().iterator(); columnIt.hasNext();) {
            printIdentifier(getColumnName((Column) columnIt.next()), ddl);
            if (columnIt.hasNext()) {
                ddl.append(",");
            }
        }
        ddl.append(") SELECT ");
        for (Iterator<Map.Entry<Column, Column>> columnsIt = columnMap.entrySet().iterator(); columnsIt
                .hasNext();) {
            Map.Entry<Column, Column> entry = columnsIt.next();

            writeCastExpression((Column) entry.getKey(), (Column) entry.getValue(), ddl);
            if (columnsIt.hasNext()) {
                ddl.append(",");
            }
        }
        ddl.append(" FROM ");
        ddl.append(getFullyQualifiedTableNameShorten(sourceTable));
        printEndOfStatement(ddl);
    }

    public LinkedHashMap<Column, Column> getCopyDataColumnMapping(Table sourceTable,
            Table targetTable) {
        LinkedHashMap<Column, Column> columns = new LinkedHashMap<Column, Column>();

        for (int idx = 0; idx < sourceTable.getColumnCount(); idx++) {
            Column sourceColumn = sourceTable.getColumn(idx);
            Column targetColumn = targetTable.findColumn(sourceColumn.getName(),
                    delimitedIdentifierModeOn);

            if (targetColumn != null) {
                columns.put(sourceColumn, targetColumn);
            }
        }
        return columns;
    }

    public LinkedHashMap<Column, Column> getCopyDataColumnOrderedMapping(Table sourceTable,
            Table targetTable) {
        LinkedHashMap<Column, Column> columns = new LinkedHashMap<Column, Column>();
        for (int idx = 0; idx < sourceTable.getColumnCount(); idx++) {
            columns.put(sourceTable.getColumn(idx), targetTable.getColumn(idx));
        }
        return columns;
    }

    /**
     * Writes a cast expression that converts the value of the source column to
     * the data type of the target column. Per default, simply the name of the
     * source column is written thereby assuming that any casts happen
     * implicitly.
     */
    protected void writeCastExpression(Column sourceColumn, Column targetColumn, StringBuilder ddl) {
        printIdentifier(getColumnName(sourceColumn), ddl);
    }

    /**
     * Processes the addition of a primary key to a table.
     */
    protected void processChange(Database currentModel, Database desiredModel,
            AddPrimaryKeyChange change, StringBuilder ddl) {
        writeExternalPrimaryKeysCreateStmt(change.getChangedTable(), change.getPrimaryKeyColumns(),
                ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    /**
     * Searches in the given table for a corresponding foreign key. If the given
     * key has no name, then a foreign key to the same table with the same
     * columns in the same order is searched. If the given key has a name, then
     * the a corresponding key also needs to have the same name, or no name at
     * all, but not a different one.
     *
     * @param table
     *            The table to search in
     * @param fk
     *            The original foreign key
     * @return The corresponding foreign key if found
     */
    protected ForeignKey findCorrespondingForeignKey(Table table, ForeignKey fk) {
        boolean caseMatters = delimitedIdentifierModeOn;
        boolean checkFkName = (fk.getName() != null) && (fk.getName().length() > 0);
        Reference[] refs = fk.getReferences();
        ArrayList<Reference> curRefs = new ArrayList<Reference>();

        for (int fkIdx = 0; fkIdx < table.getForeignKeyCount(); fkIdx++) {
            ForeignKey curFk = table.getForeignKey(fkIdx);
            boolean checkCurFkName = checkFkName && (curFk.getName() != null)
                    && (curFk.getName().length() > 0);

            if ((!checkCurFkName || areEqual(fk.getName(), curFk.getName(), caseMatters))
                    && areEqual(fk.getForeignTableName(), curFk.getForeignTableName(), caseMatters)) {
                curRefs.clear();
                CollectionUtils.addAll(curRefs, curFk.getReferences());

                // the order is not fixed, so we have to take this long way
                if (curRefs.size() == refs.length) {
                    for (int refIdx = 0; refIdx < refs.length; refIdx++) {
                        boolean found = false;

                        for (int curRefIdx = 0; !found && (curRefIdx < curRefs.size()); curRefIdx++) {
                            Reference curRef = curRefs.get(curRefIdx);

                            if ((caseMatters && refs[refIdx].equals(curRef))
                                    || (!caseMatters && refs[refIdx].equalsIgnoreCase(curRef))) {
                                curRefs.remove(curRefIdx);
                                found = true;
                            }
                        }
                    }
                    if (curRefs.isEmpty()) {
                        return curFk;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Compares the two strings.
     *
     * @param string1
     *            The first string
     * @param string2
     *            The second string
     * @param caseMatters
     *            Whether case matters in the comparison
     * @return <code>true</code> if the string are equal
     */
    protected boolean areEqual(String string1, String string2, boolean caseMatters) {
        return (caseMatters && string1.equals(string2))
                || (!caseMatters && string1.equalsIgnoreCase(string2));
    }

    /**
     * Outputs the DDL to create the table along with any non-external
     * constraints as well as with external primary keys and indices (but not
     * foreign keys).
     */
    public String createTable(Table table) {
        StringBuilder ddl = new StringBuilder();
        createTable(table, ddl, false, false);
        return ddl.toString();
    }

    /**
     * Outputs the DDL to create the table along with any non-external
     * constraints as well as with external primary keys and indices (but not
     * foreign keys).
     * @param recreate TODO
     */
    protected void createTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        writeTableCreationStmt(table, ddl);
        writeTableCreationStmtEnding(table, ddl);

        if (!databaseInfo.isIndicesEmbedded()) {
            writeExternalIndicesCreateStmt(table, ddl);
        }

        if (!databaseInfo.isPrimaryKeyEmbedded()) {
            writeExternalPrimaryKeysCreateStmt(table, table.getPrimaryKeyColumns(), ddl);
        }
    }

    /**
     * Creates the external foreignkey creation statements for all tables in the
     * database.
     *
     * @param database
     *            The database
     */
    public void createExternalForeignKeys(Database database, StringBuilder ddl) {
        for (int idx = 0; idx < database.getTableCount(); idx++) {
            createExternalForeignKeys(database, database.getTable(idx), ddl);
        }
    }

    /**
     * Creates external foreign key creation statements if necessary.
     */
    public void createExternalForeignKeys(Database database, Table table, StringBuilder ddl) {
        if (!databaseInfo.isForeignKeysEmbedded()) {
            for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
                writeExternalForeignKeyCreateStmt(database, table, table.getForeignKey(idx), ddl);
            }
        }
    }

    public String dropTables(Database database) {
        StringBuilder ddl = new StringBuilder();
        dropTables(database, ddl);
        return ddl.toString();
    }

    /**
     * Outputs the DDL required to drop the database.
     */
    public void dropTables(Database database, StringBuilder ddl) {
        // we're dropping the external foreign keys first
        for (int idx = database.getTableCount() - 1; idx >= 0; idx--) {
            Table table = database.getTable(idx);

            if ((table.getName() != null) && (table.getName().length() > 0)) {
                dropExternalForeignKeys(table, ddl);
            }
        }

        // Next we drop the tables in reverse order to avoid referencial
        // problems
        // TODO: It might be more useful to either (or both)
        // * determine an order in which the tables can be dropped safely (via
        // the foreignkeys)
        // * alter the tables first to drop the internal foreignkeys
        for (int idx = database.getTableCount() - 1; idx >= 0; idx--) {
            Table table = database.getTable(idx);

            if ((table.getName() != null) && (table.getName().length() > 0)) {
                writeTableComment(table, ddl);
                dropTable(table, ddl, false, false);
            }
        }
    }

    /**
     * Outputs the DDL required to drop the given table. This method also drops
     * foreign keys to the table.
     */
    public void dropTable(Database database, Table table, StringBuilder ddl) {
        // we're dropping the foreignkeys to the table first
        for (int idx = database.getTableCount() - 1; idx >= 0; idx--) {
            Table otherTable = database.getTable(idx);
            ForeignKey[] fks = otherTable.getForeignKeys();

            for (int fkIdx = 0; (fks != null) && (fkIdx < fks.length); fkIdx++) {
                if (fks[fkIdx].getForeignTable().equals(table)) {
                    writeExternalForeignKeyDropStmt(otherTable, fks[fkIdx], ddl);
                }
            }
        }
        // and the foreign keys from the table
        dropExternalForeignKeys(table, ddl);

        writeTableComment(table, ddl);
        dropTable(table, ddl, false, false);
    }

    /**
     * Outputs the DDL to drop the table. Note that this method does not drop
     * foreign keys to this table. Use {@link #dropTable(Database, Table)} if
     * you want that.
     * @param recreate TODO
     */
    protected void dropTable(Table table, StringBuilder ddl, boolean temporary, boolean recreate) {
        ddl.append("DROP TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(table));
        printEndOfStatement(ddl);
    }

    /**
     * Creates external foreign key drop statements.
     *
     * @param table
     *            The table
     */
    public void dropExternalForeignKeys(Table table, StringBuilder ddl) {
        if (!databaseInfo.isForeignKeysEmbedded()) {
            for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
                writeExternalForeignKeyDropStmt(table, table.getForeignKey(idx), ddl);
            }
        }
    }

    /**
     * Creates the SQL for inserting an object into the specified table. If
     * values are given then a concrete insert statement is created, otherwise
     * an insert statement usable in a prepared statement is build.
     *
     * @param table
     *            The table
     * @param columnValues
     *            The columns values indexed by the column names
     * @param genPlaceholders
     *            Whether to generate value placeholders for a prepared
     *            statement
     * @return The insertion sql
     */
    public String getInsertSql(Table table, Map<String, Object> columnValues,
            boolean genPlaceholders) {
        StringBuffer buffer = new StringBuffer("INSERT INTO ");
        boolean addComma = false;

        buffer.append(getFullyQualifiedTableNameShorten(table));
        buffer.append(" (");

        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (columnValues.containsKey(column.getName())) {
                if (addComma) {
                    buffer.append(", ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                addComma = true;
            }
        }
        buffer.append(") VALUES (");
        if (genPlaceholders) {
            addComma = false;
            for (int idx = 0; idx < columnValues.size(); idx++) {
                if (addComma) {
                    buffer.append(", ");
                }
                buffer.append("?");
                addComma = true;
            }
        } else {
            addComma = false;
            for (int idx = 0; idx < table.getColumnCount(); idx++) {
                Column column = table.getColumn(idx);

                if (columnValues.containsKey(column.getName())) {
                    if (addComma) {
                        buffer.append(", ");
                    }
                    buffer.append(getValueAsString(column, columnValues.get(column.getName())));
                    addComma = true;
                }
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    /**
     * Creates the SQL for updating an object in the specified table. If values
     * are given then a concrete update statement is created, otherwise an
     * update statement usable in a prepared statement is build.
     *
     * @param table
     *            The table
     * @param columnValues
     *            Contains the values for the columns to update, and should also
     *            contain the primary key values to identify the object to
     *            update in case <code>genPlaceholders</code> is
     *            <code>false</code>
     * @param genPlaceholders
     *            Whether to generate value placeholders for a prepared
     *            statement (both for the pk values and the object values)
     * @return The update sql
     */
    public String getUpdateSql(Table table, Map<String, Object> columnValues,
            boolean genPlaceholders) {
        StringBuffer buffer = new StringBuffer("UPDATE ");
        boolean addSep = false;

        buffer.append(getFullyQualifiedTableNameShorten(table));
        buffer.append(" SET ");

        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (!column.isPrimaryKey() && columnValues.containsKey(column.getName())) {
                if (addSep) {
                    buffer.append(", ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                buffer.append(" = ");
                if (genPlaceholders) {
                    buffer.append("?");
                } else {
                    buffer.append(getValueAsString(column, columnValues.get(column.getName())));
                }
                addSep = true;
            }
        }
        buffer.append(" WHERE ");
        addSep = false;
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            Column column = table.getColumn(idx);

            if (column.isPrimaryKey() && columnValues.containsKey(column.getName())) {
                if (addSep) {
                    buffer.append(" AND ");
                }
                buffer.append(getDelimitedIdentifier(column.getName()));
                buffer.append(" = ");
                if (genPlaceholders) {
                    buffer.append("?");
                } else {
                    buffer.append(getValueAsString(column, columnValues.get(column.getName())));
                }
                addSep = true;
            }
        }
        return buffer.toString();
    }

    /**
     * Creates the SQL for deleting an object from the specified table. If
     * values are given then a concrete delete statement is created, otherwise
     * an delete statement usable in a prepared statement is build.
     *
     * @param table
     *            The table
     * @param pkValues
     *            The primary key values indexed by the column names, can be
     *            empty
     * @param genPlaceholders
     *            Whether to generate value placeholders for a prepared
     *            statement
     * @return The delete sql
     */
    public String getDeleteSql(Table table, Map<String, Object> pkValues, boolean genPlaceholders) {
        StringBuffer buffer = new StringBuffer("DELETE FROM ");
        boolean addSep = false;

        buffer.append(getFullyQualifiedTableNameShorten(table));
        if ((pkValues != null) && !pkValues.isEmpty()) {
            buffer.append(" WHERE ");
            for (Iterator<Map.Entry<String, Object>> it = pkValues.entrySet().iterator(); it
                    .hasNext();) {
                Map.Entry<String, Object> entry = it.next();
                Column column = table.findColumn((String) entry.getKey());

                if (addSep) {
                    buffer.append(" AND ");
                }
                buffer.append(getDelimitedIdentifier(entry.getKey().toString()));
                buffer.append(" = ");
                if (genPlaceholders) {
                    buffer.append("?");
                } else {
                    buffer.append(column == null ? entry.getValue() : getValueAsString(column,
                            entry.getValue()));
                }
                addSep = true;
            }
        }
        return buffer.toString();
    }

    /**
     * Generates the string representation of the given value.
     *
     * @param column
     *            The column
     * @param value
     *            The value
     * @return The string representation
     */
    protected String getValueAsString(Column column, Object value) {
        if (value == null) {
            return "NULL";
        }

        StringBuffer result = new StringBuffer();

        // TODO: Handle binary types (BINARY, VARBINARY, LONGVARBINARY, BLOB)
        switch (column.getMappedTypeCode()) {
            case Types.DATE:
                result.append(databaseInfo.getValueQuoteToken());
                if (!(value instanceof String) && (getValueDateFormat() != null)) {
                    // TODO: Can the format method handle java.sql.Date properly
                    // ?
                    result.append(getValueDateFormat().format(value));
                } else {
                    result.append(value.toString());
                }
                result.append(databaseInfo.getValueQuoteToken());
                break;
            case Types.TIME:
                result.append(databaseInfo.getValueQuoteToken());
                if (!(value instanceof String) && (getValueTimeFormat() != null)) {
                    // TODO: Can the format method handle java.sql.Date properly
                    // ?
                    result.append(getValueTimeFormat().format(value));
                } else {
                    result.append(value.toString());
                }
                result.append(databaseInfo.getValueQuoteToken());
                break;
            case Types.TIMESTAMP:
                result.append(databaseInfo.getValueQuoteToken());
                // TODO: SimpleDateFormat does not support nano seconds so we
                // would
                // need a custom date formatter for timestamps
                result.append(value.toString());
                result.append(databaseInfo.getValueQuoteToken());
                break;
            case Types.REAL:
            case Types.NUMERIC:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
                result.append(databaseInfo.getValueQuoteToken());
                if (!(value instanceof String) && (getValueNumberFormat() != null)) {
                    result.append(getValueNumberFormat().format(value));
                } else {
                    result.append(value.toString());
                }
                result.append(databaseInfo.getValueQuoteToken());
                break;
            default:
                result.append(databaseInfo.getValueQuoteToken());
                result.append(escapeStringValue(value.toString()));
                result.append(databaseInfo.getValueQuoteToken());
                break;
        }
        return result.toString();
    }

    /**
     * Generates the SQL for querying the id that was created in the last
     * insertion operation. This is obviously only useful for pk fields that are
     * auto-incrementing. A database that does not support this, will return
     * <code>null</code>.
     *
     * @param table
     *            The table
     * @return The sql, or <code>null</code> if the database does not support
     *         this
     */
    public String getSelectLastIdentityValues(Table table) {
        // No default possible as the databases are quite different in this
        // respect
        return null;
    }

    /**
     * Generates the SQL to execute that sets the current sequence number.
     *
     * @param table
     * @param id
     * @return
     */
    public String fixLastIdentityValues(Table table) {
        return null;
    }

    public void writeFixLastIdentityValues(Table table, StringBuilder ddl) {
        String sql = fixLastIdentityValues(table);
        if (sql != null) {
            ddl.append(sql);
            printEndOfStatement(ddl);
        }
    }

    /**
     * Generates a version of the name that has at most the specified length.
     *
     * @param name
     *            The original name
     * @param desiredLength
     *            The desired maximum length
     * @return The shortened version
     */
    public String shortenName(String name, int desiredLength) {
        // TODO: Find an algorithm that generates unique names
        int originalLength = name.length();

        if ((desiredLength <= 0) || (originalLength <= desiredLength)) {
            return name;
        }

        int delta = originalLength - desiredLength;
        int startCut = desiredLength / 2;

        StringBuffer result = new StringBuffer();

        result.append(name.substring(0, startCut));
        if (((startCut == 0) || (name.charAt(startCut - 1) != '_'))
                && ((startCut + delta + 1 == originalLength) || (name.charAt(startCut + delta + 1) != '_'))) {
            // just to make sure that there isn't already a '_' right before or
            // right
            // after the cutting place (which would look odd with an aditional
            // one)
            result.append("_");
        }
        result.append(name.substring(startCut + delta + 1, originalLength));
        return result.toString();
    }

    /**
     * Returns the table name. This method takes care of length limitations
     * imposed by some databases.
     *
     * @param table
     *            The table
     * @return The table name
     */
    public String getTableName(String tableName) {
        return shortenName(tableName, databaseInfo.getMaxTableNameLength());
    }

    /**
     * Outputs a comment for the table.
     *
     * @param table
     *            The table
     */
    protected void writeTableComment(Table table, StringBuilder ddl) {
        printComment("-----------------------------------------------------------------------", ddl);
        printComment(getFullyQualifiedTableNameShorten(table), ddl);
        printComment("-----------------------------------------------------------------------", ddl);
        println(ddl);
    }

    /**
     * Generates the first part of the ALTER TABLE statement including the table
     * name.
     *
     * @param table
     *            The table being altered
     */
    protected void writeTableAlterStmt(Table table, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        ddl.append(getFullyQualifiedTableNameShorten(table));
        println(ddl);
        printIndent(ddl);
    }

    /**
     * Writes the table creation statement without the statement end.
     */
    protected void writeTableCreationStmt(Table table, StringBuilder ddl) {
        ddl.append("CREATE TABLE ");
        //TODO Check side effect with shortened name table
        ddl.append(getFullyQualifiedTableNameShorten(table));
        println("(", ddl);

        writeColumns(table, ddl);

        if (databaseInfo.isPrimaryKeyEmbedded()) {
            writeEmbeddedPrimaryKeysStmt(table, ddl);
        }
        if (databaseInfo.isForeignKeysEmbedded()) {
            writeEmbeddedForeignKeysStmt(table, ddl);
        }
        if (databaseInfo.isIndicesEmbedded()) {
            writeEmbeddedIndicesStmt(table, ddl);
        }
        println(ddl);
        ddl.append(")");

        if (isSpecifyIdentityGapLimit()) {
            writeIdentityGapLimit(ddl);
        }
    }

    /**
     * Writes the end of table statement indicating an identity gap maximum value.
     */
    protected void writeIdentityGapLimit(StringBuilder ddl) {
        int gapSize = getGapLimitSize();
        ddl.append("  with identity_gap = " + gapSize);
    }

    /**
     * Should the identity gap maximum size be specified in table create statements.
     */
    protected static boolean isSpecifyIdentityGapLimit() {
        return "true".equalsIgnoreCase(System.getProperty(
                "org.jumpmind.symmetric.ddl.gap.limit", "false"));
    }

    /**
     * Get the maximum identity gap size from the property.
     */
    protected static int getGapLimitSize() {
        return Integer.valueOf(System.getProperty(
                "org.jumpmind.symmetric.ddl.gap.max", "10000"));
    }

    /**
     * Writes the end of the table creation statement. Per default, only the end
     * of the statement is written, but this can be changed in subclasses.
     *
     * @param table
     *            The table
     */
    protected void writeTableCreationStmtEnding(Table table, StringBuilder ddl) {
        printEndOfStatement(ddl);
    }

    /**
     * Writes the columns of the given table.
     */
    protected void writeColumns(Table table, StringBuilder ddl) {
        for (int idx = 0; idx < table.getColumnCount(); idx++) {
            printIndent(ddl);
            writeColumn(table, table.getColumn(idx), ddl);
            if (idx < table.getColumnCount() - 1) {
                println(",", ddl);
            }
        }
    }

    /**
     * Returns the column name. This method takes care of length limitations
     * imposed by some databases.
     *
     * @param column
     *            The column
     * @return The column name
     */
    protected String getColumnName(Column column) {
        return shortenName(column.getName(), databaseInfo.getMaxColumnNameLength());
    }

    protected void writeColumnTypeDefaultRequired(Table table, Column column, StringBuilder ddl) {
        // see comments in columnsDiffer about null/"" defaults
        printIdentifier(getColumnName(column), ddl);
        ddl.append(" ");
        writeColumnType(table, column, ddl);
    }
    
    public String getColumnTypeDdl(Table table, Column column) {
        StringBuilder ddl = new StringBuilder();
        writeColumnType(table, column, ddl);
        return ddl.toString();
    }
    
    protected void writeColumnType(Table table, Column column, StringBuilder ddl) {
        ddl.append(getSqlType(column));
        writeColumnDefaultValueStmt(table, column, ddl);
        if (column.isRequired()) {
            ddl.append(" ");
            writeColumnNotNullableStmt(ddl);
        } else if (databaseInfo.isNullAsDefaultValueRequired()
                && databaseInfo.hasNullDefault(column.getMappedTypeCode())) {
            ddl.append(" ");
            writeColumnNullableStmt(ddl);
        }
    }

    /**
     * Outputs the DDL for the specified column.
     */
    protected void writeColumn(Table table, Column column, StringBuilder ddl) {
        writeColumnTypeDefaultRequired(table, column, ddl);
        if (column.isPrimaryKey() && databaseInfo.isPrimaryKeyEmbedded()) {
            writeColumnEmbeddedPrimaryKey(table, column, ddl);
        }

        if (column.isAutoIncrement() && !databaseInfo.isDefaultValueUsedForIdentitySpec()) {
            if (!databaseInfo.isNonPKIdentityColumnsSupported() && !column.isPrimaryKey()) {
                throw new ModelException(
                        "Column "
                                + column.getName()
                                + " in table "
                                + table.getName()
                                + " is auto-incrementing but not a primary key column, which is not supported by the platform");
            }
            ddl.append(" ");
            writeColumnAutoIncrementStmt(table, column, ddl);
        }
    }

    protected void writeColumnEmbeddedPrimaryKey(Table table, Column column, StringBuilder ddl) {

    }

    /**
     * Returns the full SQL type specification (including size and
     * precision/scale) for the given column.
     *
     * @param column
     *            The column
     * @return The full SQL type string including the size
     */
    protected String getSqlType(Column column) {
        PlatformColumn platformColumn = column.findPlatformColumn(databaseName);
        String nativeType = getNativeType(column);
        if (platformColumn != null) {
            nativeType = platformColumn.getType();
        }
        
        int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);
        StringBuilder sqlType = new StringBuilder();

        sqlType.append(sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType);

        Integer size = column.getSizeAsInt();
        if (platformColumn != null) {
            size = platformColumn.getSize();
        }
        
        if ((size == null || size == 0) && platformColumn == null) {
            size = databaseInfo.getDefaultSize(column.getMappedTypeCode());
        }
        
        int scale = column.getScale();
        if (platformColumn != null) {
            scale = platformColumn.getDecimalDigits();
        }
        
        if (size != null && size >= 0) {
            if (databaseInfo.hasSize(column.getMappedTypeCode())) {
                if (size > 0) {
                    sqlType.append("(");
                    sqlType.append(size.toString());
                    sqlType.append(")");
                }
            } else if (databaseInfo.hasPrecisionAndScale(column.getMappedTypeCode())) {
                StringBuilder precisionAndScale = new StringBuilder();
                precisionAndScale.append("(");
                precisionAndScale.append(size);
                if (scale >= 0) {
                    precisionAndScale.append(",");
                    precisionAndScale.append(scale);
                }
                precisionAndScale.append(")");
                if (!"(0,0)".equals(precisionAndScale.toString())) {
                    sqlType.append(precisionAndScale);
                }
            }
        }
        sqlType.append(sizePos >= 0 ? nativeType.substring(sizePos + SIZE_PLACEHOLDER.length())
                : "");

        filterColumnSqlType(sqlType);
        return sqlType.toString();
    }
    
    protected void filterColumnSqlType(StringBuilder sqlType) {
        // Default is to not filter but allows subclasses to filter as needed.
    }

    /**
     * Returns the database-native type for the given column.
     *
     * @param column
     *            The column
     * @return The native type
     */
    protected String getNativeType(Column column) {
        String nativeType = (String) databaseInfo.getNativeType(column.getMappedTypeCode());
        return nativeType == null ? column.getMappedType() : nativeType;
    }

    /**
     * Returns the bare database-native type for the given column without any
     * size specifies.
     *
     * @param column
     *            The column
     * @return The native type
     */
    protected String getBareNativeType(Column column) {
        String nativeType = getNativeType(column);
        int sizePos = nativeType.indexOf(SIZE_PLACEHOLDER);

        return sizePos >= 0 ? nativeType.substring(0, sizePos) : nativeType;
    }

    /**
     * Returns the native default value for the column.
     *
     * @param column
     *            The column
     * @return The native default value
     */
    protected String getNativeDefaultValue(Column column) {
        String defaultValue = column.getDefaultValue();
        PlatformColumn platformColumn = column.findPlatformColumn(databaseName);
        if (platformColumn != null) {
            defaultValue = platformColumn.getDefaultValue();
        }
        return defaultValue;
    }

    /**
     * Escapes the necessary characters in given string value.
     *
     * @param value
     *            The value
     * @return The corresponding string with the special characters properly
     *         escaped
     */
    protected String escapeStringValue(String value) {
        String result = value;

        for (Iterator<Map.Entry<String, String>> it = charSequencesToEscape.entrySet().iterator(); it
                .hasNext();) {
            Map.Entry<String, String> entry = (Map.Entry<String, String>) it.next();

            result = StringUtils.replace(result, entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Determines whether the given default spec is a non-empty spec that shall
     * be used in a DEFAULT expression. E.g. if the spec is an empty string and
     * the type is a numeric type, then it is no valid default value whereas if
     * it is a string type, then it is valid.
     *
     * @param defaultSpec
     *            The default value spec
     * @param typeCode
     *            The JDBC type code
     * @return <code>true</code> if the default value spec is valid
     */
    protected boolean isValidDefaultValue(String defaultSpec, int typeCode) {
        return (defaultSpec != null)
                && ((defaultSpec.length() > 0) || (!TypeMap.isNumericType(typeCode) && !TypeMap
                        .isDateTimeType(typeCode)));
    }

    /**
     * Prints the default value stmt part for the column.
     */
    protected void writeColumnDefaultValueStmt(Table table, Column column, StringBuilder ddl) {
        Object parsedDefault = column.getParsedDefaultValue();

        if (parsedDefault != null) {
            if (!databaseInfo.isDefaultValuesForLongTypesSupported()
                    && ((column.getMappedTypeCode() == Types.LONGVARBINARY) || (column.getMappedTypeCode() == Types.LONGVARCHAR))) {
                throw new ModelException(
                        "The platform does not support default values for LONGVARCHAR or LONGVARBINARY columns");
            }
            // we write empty default value strings only if the type is not a
            // numeric or date/time type
            if (isValidDefaultValue(column.getDefaultValue(), column.getMappedTypeCode())) {
                ddl.append(" DEFAULT ");
                writeColumnDefaultValue(table, column, ddl);
            }
        } else if (databaseInfo.isDefaultValueUsedForIdentitySpec() && column.isAutoIncrement()) { // here?
            ddl.append(" DEFAULT ");
            writeColumnDefaultValue(table, column, ddl);
        } else if (!StringUtils.isBlank(column.getDefaultValue())) {
            ddl.append(" DEFAULT ");
            writeColumnDefaultValue(table, column, ddl);
        }
    }

    /**
     * Prints the default value of the column.
     */
    protected void writeColumnDefaultValue(Table table, Column column, StringBuilder ddl) {
        printDefaultValue(getNativeDefaultValue(column), column.getMappedTypeCode(), ddl);
    }

    /**
     * Prints the default value of the column.
     */
    protected void printDefaultValue(String defaultValue, int typeCode, StringBuilder ddl) {
        boolean isNull = false;
        if (defaultValue==null || defaultValue.equalsIgnoreCase("null")) {
            isNull = true;
        }
        String defaultValueStr = mapDefaultValue(defaultValue, typeCode);
        boolean shouldUseQuotes = !isNull && !TypeMap.isNumericType(typeCode) &&
                !(TypeMap.isDateTimeType(typeCode)
                && (defaultValueStr.toUpperCase().startsWith("TO_DATE(")
                || defaultValueStr.toUpperCase().startsWith("SYSDATE")
                || defaultValueStr.toUpperCase().startsWith("SYSTIMESTAMP")
                || defaultValueStr.toUpperCase().startsWith("CURRENT_TIMESTAMP")
                || defaultValueStr.toUpperCase().startsWith("CURRENT_TIME")
                || defaultValueStr.toUpperCase().startsWith("CURRENT_DATE")
                || defaultValueStr.toUpperCase().startsWith("CURRENT_USER")
                || defaultValueStr.toUpperCase().startsWith("USER")
                || defaultValueStr.toUpperCase().startsWith("SYSTEM_USER")
                || defaultValueStr.toUpperCase().startsWith("SESSION_USER")
                || defaultValueStr.toUpperCase().startsWith("DATE '")
                || defaultValueStr.toUpperCase().startsWith("TIME '")
                || defaultValueStr.toUpperCase().startsWith("TIMESTAMP '")
                || defaultValueStr.toUpperCase().startsWith("INTERVAL '")
                )) &&
                !(defaultValueStr.toUpperCase().startsWith("N'") && defaultValueStr.endsWith("'"));

        if (shouldUseQuotes) {
            // characters are only escaped when within a string literal
            ddl.append(databaseInfo.getValueQuoteToken());
            ddl.append(escapeStringValue(defaultValueStr));
            ddl.append(databaseInfo.getValueQuoteToken());
        } else {
            ddl.append(defaultValueStr);
        }
    }

    protected String mapDefaultValue(Object defaultValue, int typeCode) {
        if (defaultValue == null) {
            defaultValue = "NULL";
        }
        return defaultValue.toString();
    }

    /**
     * Prints that the column is an auto increment column.
     */
    protected void writeColumnAutoIncrementStmt(Table table, Column column, StringBuilder ddl) {
        ddl.append("IDENTITY");
    }

    /**
     * Prints that a column is nullable.
     */
    protected void writeColumnNullableStmt(StringBuilder ddl) {
        ddl.append("NULL");
    }

    /**
     * Prints that a column is not nullable.
     */
    protected void writeColumnNotNullableStmt(StringBuilder ddl) {
        ddl.append("NOT NULL");
    }

    /**
     * Compares the current column in the database with the desired one. Type,
     * nullability, size, scale, default value, and precision radix are the
     * attributes checked. Currently default values are compared, and null and
     * empty string are considered equal.
     *
     * @param currentColumn
     *            The current column as it is in the database
     * @param desiredColumn
     *            The desired column
     * @return <code>true</code> if the column specifications differ
     */
    protected boolean columnsDiffer(Column currentColumn, Column desiredColumn) {
        // The createColumn method leaves off the default clause if
        // column.getDefaultValue()
        // is null. mySQL interprets this as a default of "" or 0, and thus the
        // columns
        // are always different according to this method. alterDatabase will
        // generate
        // an alter statement for the column, but it will be the exact same
        // definition
        // as before. In order to avoid this situation I am ignoring the
        // comparison
        // if the desired default is null. In order to "un-default" a column
        // you'll
        // have to have a default="" or default="0" in the schema xml.
        // If this is bad for other databases, it is recommended that the
        // createColumn
        // method use a "DEFAULT NULL" statement if that is what is needed.
        // A good way to get this would be to require a defaultValue="<NULL>" in
        // the
        // schema xml if you really want null and not just unspecified.

        String desiredDefault = desiredColumn.getDefaultValue();
        String currentDefault = currentColumn.getDefaultValue();
        boolean defaultsEqual = (desiredDefault == null) || desiredDefault.equals(currentDefault);
        boolean sizeMatters = databaseInfo.hasSize(currentColumn.getMappedTypeCode())
                && (desiredColumn.getSize() != null);

        // We're comparing the jdbc type that corresponds to the native type for
        // the
        // desired type, in order to avoid repeated altering of a perfectly
        // valid column
        if ((databaseInfo.getTargetJdbcType(desiredColumn.getMappedTypeCode()) != currentColumn
                .getMappedTypeCode())
                || (desiredColumn.isRequired() != currentColumn.isRequired())
                || (sizeMatters && !StringUtils.equals(desiredColumn.getSize(),
                        currentColumn.getSize())) || !defaultsEqual) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns the name to be used for the given foreign key. If the foreign key
     * has no specified name, this method determines a unique name for it. The
     * name will also be shortened to honor the maximum identifier length
     * imposed by the platform.
     *
     * @param table
     *            The table for whith the foreign key is defined
     * @param fk
     *            The foreign key
     * @return The name
     */
    public String getForeignKeyName(Table table, ForeignKey fk) {
        String fkName = fk.getName();
        boolean needsName = (fkName == null) || (fkName.length() == 0);

        if (needsName) {
            StringBuffer name = new StringBuffer();

            for (int idx = 0; idx < fk.getReferenceCount(); idx++) {
                name.append(fk.getReference(idx).getLocalColumnName());
                name.append("_");
            }
            name.append(fk.getForeignTableName());
            fkName = getConstraintName(null, table, "FK", name.toString());
        }
        fkName = shortenName(fkName, databaseInfo.getMaxForeignKeyNameLength());

        if (needsName) {
            log.warn("Encountered a foreign key in table " + table.getName()
                    + " that has no name. "
                    + "DdlUtils will use the auto-generated and shortened name " + fkName
                    + " instead.");
        }

        return fkName;
    }

    /**
     * Returns the constraint name. This method takes care of length limitations
     * imposed by some databases.
     *
     * @param prefix
     *            The constraint prefix, can be <code>null</code>
     * @param table
     *            The table that the constraint belongs to
     * @param secondPart
     *            The second name part, e.g. the name of the constraint column
     * @param suffix
     *            The constraint suffix, e.g. a counter (can be
     *            <code>null</code>)
     * @return The constraint name
     */
    public String getConstraintName(String prefix, Table table, String secondPart, String suffix) {
        StringBuffer result = new StringBuffer();

        if (prefix != null) {
            result.append(prefix);
            result.append("_");
        }
        result.append(table.getName());
        result.append("_");
        result.append(secondPart);
        if (suffix != null) {
            result.append("_");
            result.append(suffix);
        }
        return shortenName(result.toString(), databaseInfo.getMaxConstraintNameLength());
    }

    /**
     * Writes the primary key constraints of the table inside its definition.
     *
     * @param table
     *            The table
     */
    protected void writeEmbeddedPrimaryKeysStmt(Table table, StringBuilder ddl) {
        Column[] primaryKeyColumns = table.getPrimaryKeyColumns();

        if ((primaryKeyColumns.length > 0) && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
            printStartOfEmbeddedStatement(ddl);
            writePrimaryKeyStmt(table, primaryKeyColumns, ddl);
        }
    }

    /**
     * Writes the primary key constraints of the table as alter table
     * statements.
     *
     * @param table
     *            The table
     * @param primaryKeyColumns
     *            The primary key columns
     */
    protected void writeExternalPrimaryKeysCreateStmt(Table table, Column[] primaryKeyColumns,
            StringBuilder ddl) {
        if ((primaryKeyColumns.length > 0) && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
            ddl.append("ALTER TABLE ");
            ddl.append(getFullyQualifiedTableNameShorten(table));
            println(ddl);
            printIndent(ddl);
            ddl.append("ADD CONSTRAINT ");
            printIdentifier(getConstraintName(null, table, "PK", null), ddl);
            ddl.append(" ");
            writePrimaryKeyStmt(table, primaryKeyColumns, ddl);
            printEndOfStatement(ddl);
        }
    }

    /**
     * Determines whether we should generate a primary key constraint for the
     * given primary key columns.
     *
     * @param primaryKeyColumns
     *            The pk columns
     * @return <code>true</code> if a pk statement should be generated for the
     *         columns
     */
    protected boolean shouldGeneratePrimaryKeys(Column[] primaryKeyColumns) {
        return true;
    }

    /**
     * Writes a primary key statement for the given columns.
     *
     * @param table
     *            The table
     * @param primaryKeyColumns
     *            The primary columns
     */
    protected void writePrimaryKeyStmt(Table table, Column[] primaryKeyColumns, StringBuilder ddl) {
        ddl.append("PRIMARY KEY (");
        for (int idx = 0; idx < primaryKeyColumns.length; idx++) {
            printIdentifier(getColumnName(primaryKeyColumns[idx]), ddl);
            if (idx < primaryKeyColumns.length - 1) {
                ddl.append(", ");
            }
        }
        ddl.append(")");
    }

    /**
     * Returns the index name. This method takes care of length limitations
     * imposed by some databases.
     *
     * @param index
     *            The index
     * @return The index name
     */
    public String getIndexName(IIndex index) {
        return shortenName(index.getName(), databaseInfo.getMaxConstraintNameLength());
    }

    /**
     * Writes the indexes of the given table.
     *
     * @param table
     *            The table
     */
    protected void writeExternalIndicesCreateStmt(Table table, StringBuilder ddl) {
        for (int idx = 0; idx < table.getIndexCount(); idx++) {
            IIndex index = table.getIndex(idx);

            if (!index.isUnique() && !databaseInfo.isIndicesSupported()) {
                return;
            }
            writeExternalIndexCreateStmt(table, index, ddl);
        }
    }

    /**
     * Writes the indexes embedded within the create table statement.
     */
    protected void writeEmbeddedIndicesStmt(Table table, StringBuilder ddl) {
        if (databaseInfo.isIndicesSupported()) {
            for (int idx = 0; idx < table.getIndexCount(); idx++) {
                printStartOfEmbeddedStatement(ddl);
                writeEmbeddedIndexCreateStmt(table, table.getIndex(idx), ddl);
            }
        }
    }

    /**
     * Writes the given index of the table.
     */
    protected void writeExternalIndexCreateStmt(Table table, IIndex index, StringBuilder ddl) {
        if (databaseInfo.isIndicesSupported()) {
            if (index.getName() == null) {
                log.warn("Cannot write unnamed index " + index);
            } else {
                ddl.append("CREATE");
                if (index.isUnique()) {
                    ddl.append(" UNIQUE");
                }
                ddl.append(" INDEX ");
                printIdentifier(getIndexName(index), ddl);
                ddl.append(" ON ");
                ddl.append(getFullyQualifiedTableNameShorten(table));
                ddl.append(" (");

                for (int idx = 0; idx < index.getColumnCount(); idx++) {
                    IndexColumn idxColumn = index.getColumn(idx);
                    if (idx > 0) {
                        ddl.append(", ");
                    }
                    String name = shortenName(idxColumn.getName(), databaseInfo.getMaxColumnNameLength());
                    if (name.startsWith("(")) {
                        // When a column name starts with parenthesis, it's a grouping syntax for an aggregate column, so don't quote it
                        ddl.append(name);
                    } else {
                        printIdentifier(name, ddl);
                    }
                }

                ddl.append(")");
                printEndOfStatement(ddl);
            }
        }
    }

    /**
     * Writes the given embedded index of the table.
     */
    protected void writeEmbeddedIndexCreateStmt(Table table, IIndex index, StringBuilder ddl) {
        if ((index.getName() != null) && (index.getName().length() > 0)) {
            ddl.append(" CONSTRAINT ");
            printIdentifier(getIndexName(index), ddl);
        }
        if (index.isUnique()) {
            ddl.append(" UNIQUE");
        } else {
            ddl.append(" INDEX ");
        }
        ddl.append(" (");

        for (int idx = 0; idx < index.getColumnCount(); idx++) {
            IndexColumn idxColumn = index.getColumn(idx);
            Column col = table.findColumn(idxColumn.getName());

            if (col == null) {
                // would get null pointer on next line anyway, so throw
                // exception
                throw new ModelException("Invalid column '" + idxColumn.getName() + "' on index "
                        + index.getName() + " for table " + table.getName());
            }
            if (idx > 0) {
                ddl.append(", ");
            }
            printIdentifier(getColumnName(col), ddl);
        }

        ddl.append(")");
    }

    /**
     * Generates the statement to drop a non-embedded index from the database.
     */
    public void writeExternalIndexDropStmt(Table table, IIndex index, StringBuilder ddl) {
        if (databaseInfo.isAlterTableForDropUsed()) {
            writeTableAlterStmt(table, ddl);
        }
        ddl.append("DROP INDEX ");
        printIdentifier(getIndexName(index), ddl);
        if (!databaseInfo.isAlterTableForDropUsed()) {
            ddl.append(" ON ");
            ddl.append(getFullyQualifiedTableNameShorten(table));
        }
        printEndOfStatement(ddl);
    }

    /**
     * Writes the foreign key constraints inside a create table () clause.
     */
    protected void writeEmbeddedForeignKeysStmt(Table table, StringBuilder ddl) {
        for (int idx = 0; idx < table.getForeignKeyCount(); idx++) {
            ForeignKey key = table.getForeignKey(idx);

            if (key.getForeignTableName() == null) {
                log.warn("Foreign key table is null for key " + key);
            } else {
                printStartOfEmbeddedStatement(ddl);
                if (databaseInfo.isEmbeddedForeignKeysNamed()) {
                    ddl.append("CONSTRAINT ");
                    printIdentifier(getForeignKeyName(table, key), ddl);
                    ddl.append(" ");
                }
                ddl.append("FOREIGN KEY (");
                writeLocalReferences(key, ddl);
                ddl.append(") REFERENCES ");
                if (StringUtils.isNotBlank(table.getCatalog())) {
                    ddl.append(getDelimitedIdentifier(table.getCatalog())).append(".");
                }
                if (StringUtils.isNotBlank(table.getSchema())) {
                    ddl.append(getDelimitedIdentifier(table.getSchema())).append(".");
                }
                printIdentifier(getTableName(key.getForeignTableName()), ddl);
                ddl.append(" (");
                writeForeignReferences(key, ddl);
                ddl.append(")");
            }
        }
    }

    /**
     * Writes a single foreign key constraint using a alter table statement.
     *
     * @param database
     *            The database model
     * @param table
     *            The table
     * @param key
     *            The foreign key
     */
    protected void writeExternalForeignKeyCreateStmt(Database database, Table table,
            ForeignKey key, StringBuilder ddl) {
        if (key.getForeignTableName() == null) {
            log.warn("Foreign key table is null for key " + key);
        } else {
            writeTableAlterStmt(table, ddl);
            ddl.append("ADD CONSTRAINT ");
            printIdentifier(getForeignKeyName(table, key), ddl);
            ddl.append(" FOREIGN KEY (");
            writeLocalReferences(key, ddl);
            ddl.append(") REFERENCES ");
            // TODO: use getDelimitedIdentifier() around catalog/schema, but we'll need to get the case right
            // TODO : use same catalog and schema than table because catalog and schema are not present in table object
            if (StringUtils.isNotBlank(table.getCatalog())) {
                ddl.append(table.getCatalog()).append(".");
            }
            if (StringUtils.isNotBlank(table.getSchema())) {
                ddl.append(table.getSchema()).append(".");
            }
            printIdentifier(getTableName(key.getForeignTableName()), ddl);
            ddl.append(" (");
            writeForeignReferences(key, ddl);
            ddl.append(")");
            printEndOfStatement(ddl);
        }
    }

    /**
     * Writes a list of local references for the given foreign key.
     *
     * @param key
     *            The foreign key
     */
    protected void writeLocalReferences(ForeignKey key, StringBuilder ddl) {
        for (int idx = 0; idx < key.getReferenceCount(); idx++) {
            if (idx > 0) {
                ddl.append(", ");
            }
            printIdentifier(key.getReference(idx).getLocalColumnName(), ddl);
        }
    }

    /**
     * Writes a list of foreign references for the given foreign key.
     *
     * @param key
     *            The foreign key
     */
    protected void writeForeignReferences(ForeignKey key, StringBuilder ddl) {
        for (int idx = 0; idx < key.getReferenceCount(); idx++) {
            if (idx > 0) {
                ddl.append(", ");
            }
            printIdentifier(key.getReference(idx).getForeignColumnName(), ddl);
        }
    }

    /**
     * Generates the statement to drop a foreignkey constraint from the database
     * using an alter table statement.
     *
     * @param table
     *            The table
     * @param foreignKey
     *            The foreign key
     */
    protected void writeExternalForeignKeyDropStmt(Table table, ForeignKey foreignKey,
            StringBuilder ddl) {
        writeTableAlterStmt(table, ddl);
        ddl.append("DROP CONSTRAINT ");
        printIdentifier(getForeignKeyName(table, foreignKey), ddl);
        printEndOfStatement(ddl);
    }

    /**
     * Prints an SQL comment to the current stream.
     *
     * @param text
     *            The comment text
     */
    protected void printComment(String text, StringBuilder ddl) {
        if (sqlCommentsOn) {
            ddl.append(databaseInfo.getCommentPrefix());
            // Some databases insist on a space after the prefix
            ddl.append(" ");
            ddl.append(text);
            ddl.append(" ");
            ddl.append(databaseInfo.getCommentSuffix());
            println(ddl);
        }
    }

    /**
     * Prints the start of an embedded statement.
     */
    protected void printStartOfEmbeddedStatement(StringBuilder ddl) {
        println(",", ddl);
        printIndent(ddl);
    }

    /**
     * Prints the end of statement text, which is typically a semi colon
     * followed by a carriage return.
     */
    protected void printEndOfStatement(StringBuilder ddl) {
        println(databaseInfo.getSqlCommandDelimiter(), ddl);
    }

    /**
     * Prints a newline.
     */
    protected void println(StringBuilder ddl) {
        ddl.append(LINE_SEPARATOR);
    }

    /**
     * Returns the delimited version of the identifier (if configured).
     *
     * @param identifier
     *            The identifier
     * @return The delimited version of the identifier unless the platform is
     *         configured to use undelimited identifiers; in that case, the
     *         identifier is returned unchanged
     */
    protected String getDelimitedIdentifier(String identifier) {
        if (delimitedIdentifierModeOn) {
            return databaseInfo.getDelimiterToken() + identifier + databaseInfo.getDelimiterToken();
        } else {
            return identifier;
        }
    }

    /**
     * Prints the given identifier. For most databases, this will be a delimited
     * identifier.
     *
     * @param identifier
     *            The identifier
     */
    protected void printIdentifier(String identifier, StringBuilder ddl) {
        ddl.append(getDelimitedIdentifier(identifier));
    }

    /**
     * Prints the given identifier followed by a newline. For most databases,
     * this will be a delimited identifier.
     *
     * @param identifier
     *            The identifier
     */
    protected void printlnIdentifier(String identifier, StringBuilder ddl) {
        println(getDelimitedIdentifier(identifier), ddl);
    }

    /**
     * Prints some text followed by a newline.
     *
     * @param text
     *            The text to print
     */
    protected void println(String text, StringBuilder ddl) {
        ddl.append(text);
        println(ddl);
    }

    /**
     * Prints the characters used to indent SQL.
     */
    protected void printIndent(StringBuilder ddl) {
        ddl.append(getIndent());
    }

    /*
     * Determines whether script mode is on. This means that the generated SQL
     * is not intended to be sent directly to the database but rather to be
     * saved in a SQL script file. Per default, script mode is off.
     *
     * @return <code>true</code> if script mode is on
     */
    public boolean isScriptModeOn() {
        return scriptModeOn;
    }

    /*
     * Specifies whether script mode is on. This means that the generated SQL is
     * not intended to be sent directly to the database but rather to be saved
     * in a SQL script file.
     *
     * @param scriptModeOn <code>true</code> if script mode is on
     */
    public void setScriptModeOn(boolean scriptModeOn) {
        this.scriptModeOn = scriptModeOn;
    }

    /*
     * Determines whether SQL comments are generated.
     *
     * @return <code>true</code> if SQL comments shall be generated
     */
    public boolean isSqlCommentsOn() {
        return sqlCommentsOn;
    }

    /*
     * Specifies whether SQL comments shall be generated.
     *
     * @param sqlCommentsOn <code>true</code> if SQL comments shall be generated
     */
    public void setSqlCommentsOn(boolean sqlCommentsOn) {
        if (!databaseInfo.isSqlCommentsSupported() && sqlCommentsOn) {
            throw new DdlException("Platform does not support SQL comments");
        }
        this.sqlCommentsOn = sqlCommentsOn;
    }

    /*
     * Determines whether delimited identifiers are used or normal SQL92
     * identifiers (which may only contain alphanumerical characters and the
     * underscore, must start with a letter and cannot be a reserved keyword).
     *
     * @return <code>true</code> if delimited identifiers are used
     */
    public boolean isDelimitedIdentifierModeOn() {
        return delimitedIdentifierModeOn;
    }

    /*
     * Specifies whether delimited identifiers are used or normal SQL92
     * identifiers.
     *
     * @param delimitedIdentifierModeOn <code>true</code> if delimited
     * identifiers shall be used
     */
    public void setDelimitedIdentifierModeOn(boolean delimitedIdentifierModeOn) {
        if (!databaseInfo.isDelimitedIdentifiersSupported() && delimitedIdentifierModeOn) {
            log.info("Platform does not support delimited identifier.  Delimited identifiers will not be enabled.");
        } else {
            this.delimitedIdentifierModeOn = delimitedIdentifierModeOn;
        }

    }

    public DatabaseInfo getDatabaseInfo() {
        return databaseInfo;
    }
    
    public void setCaseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }
    
    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public String defineSavepoint(String savepointName) {
        throw new DdlException("Platform does not support SQL savepoints.");
    }

    public String rollbackToSavepoint(String savepointName) {
        throw new DdlException("Platform does not support SQL savepoints.");
    }
}
