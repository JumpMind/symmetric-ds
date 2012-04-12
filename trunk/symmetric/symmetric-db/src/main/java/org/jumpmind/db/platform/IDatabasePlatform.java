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

import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.util.BinaryEncoding;

/*
 * A platform encapsulates the database-related functionality such as performing queries
 * and manipulations. It also contains an sql builder that is specific to this platform.
 */
public interface IDatabasePlatform {

    public String getName();

    /*
     * Returns the info object for this platform.
     * 
     * @return The info object
     */
    public DatabasePlatformInfo getPlatformInfo();

    /*
     * Returns a new ddl builder for the this platform.
     */
    public IDdlBuilder getDdlBuilder();

    /*
     * Returns the model reader (which reads a database model from a live
     * database) for this platform.
     * 
     * @return The model reader
     */
    public IDdlReader getDdlReader();

    public ISqlTemplate getSqlTemplate();

/**
     * The amount of time table metadata will be cached when using {@link IDatabasePlatform#getT
     * @param clearCacheModelTimeoutInMs
     */
    public void setClearCacheModelTimeoutInMs(long clearCacheModelTimeoutInMs);

    public long getClearCacheModelTimeoutInMs();

    public String getDefaultSchema();

    public String getDefaultCatalog();

    /*
     * Determines whether script mode is on. This means that the generated SQL
     * is not intended to be sent directly to the database but rather to be
     * saved in a SQL script file. Per default, script mode is off.
     * 
     * @return <code>true</code> if script mode is on
     */
    public boolean isScriptModeOn();

    /*
     * Specifies whether script mode is on. This means that the generated SQL is
     * not intended to be sent directly to the database but rather to be saved
     * in a SQL script file.
     * 
     * @param scriptModeOn <code>true</code> if script mode is on
     */
    public void setScriptModeOn(boolean scriptModeOn);

    /*
     * Determines whether delimited identifiers are used or normal SQL92
     * identifiers (which may only contain alphanumerical characters and the
     * underscore, must start with a letter and cannot be a reserved keyword).
     * Per default, delimited identifiers are not used
     * 
     * @return <code>true</code> if delimited identifiers are used
     */
    public boolean isDelimitedIdentifierModeOn();

    /*
     * Specifies whether delimited identifiers are used or normal SQL92
     * identifiers.
     * 
     * @param delimitedIdentifierModeOn <code>true</code> if delimited
     * identifiers shall be used
     */
    public void setDelimitedIdentifierModeOn(boolean delimitedIdentifierModeOn);

    /*
     * Determines whether SQL comments are generated.
     * 
     * @return <code>true</code> if SQL comments shall be generated
     */
    public boolean isSqlCommentsOn();

    /*
     * Specifies whether SQL comments shall be generated.
     * 
     * @param sqlCommentsOn <code>true</code> if SQL comments shall be generated
     */
    public void setSqlCommentsOn(boolean sqlCommentsOn);

    /*
     * Determines whether SQL insert statements can specify values for identity
     * columns. This setting is only relevant if the database supports it
     * ({@link PlatformInfo#isIdentityOverrideAllowed()}). If this is off, then
     * the <code>insert</code> methods will ignore values for identity columns.
     * 
     * @return <code>true</code> if identity override is enabled (the default)
     */
    public boolean isIdentityOverrideOn();

    /*
     * Specifies whether SQL insert statements can specify values for identity
     * columns. This setting is only relevant if the database supports it
     * ({@link PlatformInfo#isIdentityOverrideAllowed()}). If this is off, then
     * the <code>insert</code> methods will ignore values for identity columns.
     * 
     * @param identityOverrideOn <code>true</code> if identity override is
     * enabled (the default)
     */
    public void setIdentityOverrideOn(boolean identityOverrideOn);

    /*
     * Determines whether foreign keys of a table read from a live database are
     * alphabetically sorted.
     * 
     * @return <code>true</code> if read foreign keys are sorted
     */
    public boolean isForeignKeysSorted();

    /*
     * Specifies whether foreign keys read from a live database, shall be
     * alphabetically sorted.
     * 
     * @param foreignKeysSorted <code>true</code> if read foreign keys shall be
     * sorted
     */
    public void setForeignKeysSorted(boolean foreignKeysSorted);

    /*
     * Reads the database model from the live database to which the given
     * connection is pointing.
     */
    public Database readDatabase(String catalog, String schema, String[] tableTypes);

    public Table readTableFromDatabase(String catalogName, String schemaName, String tablename);

    public void resetCachedTableModel();

    public Table getTableFromCache(String tableName, boolean forceReread);

    public Table getTableFromCache(String catalogName, String schemaName, String tableName,
            boolean forceReread);

    public void createDatabase(Database targetDatabase, boolean dropTablesFirst,
            boolean continueOnError);

    public void alterDatabase(Database desiredDatabase, boolean continueOnError);

    public void alterTables(boolean continueOnError, Table... desiredTables);

    public void dropDatabase(Database database, boolean continueOnError);

    public DmlStatement createDmlStatement(DmlType dmlType, Table table);

    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns);

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData);

    public Object[] getObjectValues(BinaryEncoding encoding, Table table, String[] columnNames,
            String[] values);
    
    public String[] getStringValues(Column[] metaData, Row row);

    public Database readDatabaseFromXml(String filePath, boolean alterCaseToMatchDatabaseDefaultCase);

    public boolean isLob(int type);

    public boolean isClob(int type);

    public boolean isBlob(int type);
    
    public List<Column> getLobColumns(Table table);

    public Map<String, String> getSqlScriptReplacementTokens();
    
    public String scrubSql(String sql);
    
    public boolean isStoresUpperCaseIdentifiers();
    
    public boolean isStoresMixedCaseQuotedIdentifiers();
    
    public <T> T getDataSource();
    
    public void setMetadataIgnoreCase(boolean value);
    
    public boolean isMetadataIgnoreCase();

}
