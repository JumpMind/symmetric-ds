package org.jumpmind.symmetric.ddl;

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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.CreationParameters;
import org.jumpmind.symmetric.ddl.platform.JdbcModelReader;
import org.jumpmind.symmetric.ddl.platform.SqlBuilder;

/*
 * A platform encapsulates the database-related functionality such as performing queries
 * and manipulations. It also contains an sql builder that is specific to this platform.
 * 
 * @version $Revision: 231110 $
 */
public interface Platform
{
    /*
     * Returns the name of the database that this platform is for.
     * 
     * @return The name
     */
    public String getName();

    /*
     * Returns the info object for this platform.
     * 
     * @return The info object
     */
    public PlatformInfo getPlatformInfo();
    
    /*
     * Returns the sql builder for the this platform.
     * 
     * @return The sql builder
     */
    public SqlBuilder getSqlBuilder();

    /*
     * Returns the model reader (which reads a database model from a live database) for this platform.
     * 
     * @return The model reader
     */
    public JdbcModelReader getModelReader();
    
    /*
     * Returns the data source that this platform uses to access the database.
     * 
     * @return The data source
     */
    public DataSource getDataSource();
    
    /*
     * Sets the data source that this platform shall use to access the database.
     * 
     * @param dataSource The data source
     */
    public void setDataSource(DataSource dataSource);

    /*
     * Returns the username that this platform shall use to access the database.
     * 
     * @return The username
     */
    public String getUsername();

    /*
     * Sets the username that this platform shall use to access the database.
     * 
     * @param username The username
     */
    public void setUsername(String username);

    /*
     * Returns the password that this platform shall use to access the database.
     * 
     * @return The password
     */
    public String getPassword();

    /*
     * Sets the password that this platform shall use to access the database.
     * 
     * @param password The password
     */
    public void setPassword(String password);

    /*
     * Determines whether script mode is on. This means that the generated SQL is not
     * intended to be sent directly to the database but rather to be saved in a SQL
     * script file. Per default, script mode is off.
     * 
     * @return <code>true</code> if script mode is on
     */
    public boolean isScriptModeOn();

    /*
     * Specifies whether script mode is on. This means that the generated SQL is not
     * intended to be sent directly to the database but rather to be saved in a SQL
     * script file.
     * 
     * @param scriptModeOn <code>true</code> if script mode is on
     */
    public void setScriptModeOn(boolean scriptModeOn);

    /*
     * Determines whether delimited identifiers are used or normal SQL92 identifiers
     * (which may only contain alphanumerical characters and the underscore, must start
     * with a letter and cannot be a reserved keyword).
     * Per default, delimited identifiers are not used
     *
     * @return <code>true</code> if delimited identifiers are used
     */
    public boolean isDelimitedIdentifierModeOn();

    /*
     * Specifies whether delimited identifiers are used or normal SQL92 identifiers.
     *
     * @param delimitedIdentifierModeOn <code>true</code> if delimited identifiers shall be used
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
     * Determines whether SQL insert statements can specify values for identity columns.
     * This setting is only relevant if the database supports it
     * ({@link PlatformInfo#isIdentityOverrideAllowed()}). If this is off, then the
     * <code>insert</code> methods will ignore values for identity columns. 
     *  
     * @return <code>true</code> if identity override is enabled (the default)
     */
    public boolean isIdentityOverrideOn();

    /*
     * Specifies whether SQL insert statements can specify values for identity columns.
     * This setting is only relevant if the database supports it
     * ({@link PlatformInfo#isIdentityOverrideAllowed()}). If this is off, then the
     * <code>insert</code> methods will ignore values for identity columns. 
     *  
     * @param identityOverrideOn <code>true</code> if identity override is enabled (the default)
     */
    public void setIdentityOverrideOn(boolean identityOverrideOn);

    /*
     * Determines whether foreign keys of a table read from a live database
     * are alphabetically sorted.
     *
     * @return <code>true</code> if read foreign keys are sorted
     */
    public boolean isForeignKeysSorted();

    /*
     * Specifies whether foreign keys read from a live database, shall be
     * alphabetically sorted.
     *
     * @param foreignKeysSorted <code>true</code> if read foreign keys shall be sorted
     */
    public void setForeignKeysSorted(boolean foreignKeysSorted);
    
    /*
     * Returns a (new) JDBC connection from the data source.
     * 
     * @return The connection
     */
    public Connection borrowConnection() throws DatabaseOperationException;

    /*
     * Closes the given JDBC connection (returns it back to the pool if the datasource is poolable).
     * 
     * @param connection The connection
     */
    public void returnConnection(Connection connection);

    /*
     * Performs a shutdown at the database. This is necessary for some embedded databases which otherwise
     * would be locked and thus would refuse other connections. Note that this does not change the database
     * structure or data in it in any way.
     */
    public void shutdownDatabase() throws DatabaseOperationException;

    /*
     * Performs a shutdown at the database. This is necessary for some embedded databases which otherwise
     * would be locked and thus would refuse other connections. Note that this does not change the database
     * structure or data in it in any way.
     * 
     * @param connection The connection to the database
     */
    public void shutdownDatabase(Connection connection) throws DatabaseOperationException;

    /*
     * Creates the database specified by the given parameters. Please note that this method does not
     * use a data source set via {@link #setDataSource(DataSource)} because it is not possible to
     * retrieve the connection information from it without establishing a connection.<br/>
     * The given connection url is the url that you'd use to connect to the already-created
     * database.<br/>
     * On some platforms, this method suppurts additional parameters. These are documented in the
     * manual section for the individual platforms. 
     * 
     * @param jdbcDriverClassName The jdbc driver class name
     * @param connectionUrl       The url to connect to the database if it were already created
     * @param username            The username for creating the database
     * @param password            The password for creating the database
     * @param parameters          Additional parameters relevant to database creation (which are platform specific)
     */
    public void createDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password, Map parameters) throws DatabaseOperationException, UnsupportedOperationException;

    /*
     * Drops the database specified by the given parameters. Please note that this method does not
     * use a data source set via {@link #setDataSource(DataSource)} because it is not possible to
     * retrieve the connection information from it without establishing a connection.
     * 
     * @param jdbcDriverClassName The jdbc driver class name
     * @param connectionUrl       The url to connect to the database
     * @param username            The username for creating the database
     * @param password            The password for creating the database
     */
    public void dropDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password) throws DatabaseOperationException, UnsupportedOperationException;

    /*
     * Creates the tables defined in the database model.
     * 
     * @param model           The database model
     * @param dropTablesFirst Whether to drop the tables prior to creating them (anew)
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void createTables(Database model, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Creates the tables defined in the database model.
     * 
     * @param connection      The connection to the database
     * @param model           The database model
     * @param dropTablesFirst Whether to drop the tables prior to creating them (anew)
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void createTables(Connection connection, Database model, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for creating the tables defined in the database model.
     * 
     * @param model           The database model
     * @param dropTablesFirst Whether to drop the tables prior to creating them (anew)
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     * @return The SQL statements
     */
    public String getCreateTablesSql(Database model, boolean dropTablesFirst, boolean continueOnError);

    /*
     * Creates the tables defined in the database model.
     * 
     * @param model           The database model
     * @param params          The parameters used in the creation
     * @param dropTablesFirst Whether to drop the tables prior to creating them (anew)
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void createTables(Database model, CreationParameters params, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Creates the tables defined in the database model.
     * 
     * @param connection      The connection to the database
     * @param model           The database model
     * @param params          The parameters used in the creation
     * @param dropTablesFirst Whether to drop the tables prior to creating them (anew)
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void createTables(Connection connection, Database model, CreationParameters params, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for creating the tables defined in the database model.
     * 
     * @param model           The database model
     * @param params          The parameters used in the creation
     * @param dropTablesFirst Whether to drop the tables prior to creating them (anew)
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     * @return The SQL statements
     */
    public String getCreateTablesSql(Database model, CreationParameters params, boolean dropTablesFirst, boolean continueOnError);

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param desiredDb       The desired database schema
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(Database desiredDb, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param desiredDb The desired database schema
     * @return The SQL statements
     */
    public String getAlterTablesSql(Database desiredDb) throws DatabaseOperationException;

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param desiredDb       The desired database schema
     * @param params          The parameters used in the creation
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(Database desiredDb, CreationParameters params, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param desiredDb The desired database schema
     * @param params    The parameters used in the creation
     * @return The SQL statements
     */
    public String getAlterTablesSql(Database desiredDb, CreationParameters params) throws DatabaseOperationException;

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param catalog         The catalog in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param schema          The schema in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param tableTypes      The table types to read from the existing database;
     *                        use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb       The desired database schema
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(String catalog, String schema, String[] tableTypes, Database desiredDb, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param catalog    The catalog in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param schema     The schema in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param tableTypes The table types to read from the existing database;
     *                   use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb  The desired database schema
     * @return The SQL statements
     */
    public String getAlterTablesSql(String catalog, String schema, String[] tableTypes, Database desiredDb) throws DatabaseOperationException;

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param catalog         The catalog in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param schema          The schema in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param tableTypes      The table types to read from the existing database;
     *                        use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb       The desired database schema
     * @param params          The parameters used in the creation
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(String catalog, String schema, String[] tableTypes, Database desiredDb, CreationParameters params, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param catalog    The catalog in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param schema     The schema in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param tableTypes The table types to read from the existing database;
     *                   use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb  The desired database schema
     * @param params     The parameters used in the creation
     * @return The SQL statements
     */
    public String getAlterTablesSql(String catalog, String schema, String[] tableTypes, Database desiredDb, CreationParameters params) throws DatabaseOperationException;

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param connection      A connection to the existing database that shall be modified
     * @param desiredDb       The desired database schema
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(Connection connection, Database desiredDb, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param connection A connection to the existing database that shall be modified
     * @param desiredDb  The desired database schema
     * @return The SQL statements
     */
    public String getAlterTablesSql(Connection connection, Database desiredDb) throws DatabaseOperationException;

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param connection      A connection to the existing database that shall be modified
     * @param desiredDb       The desired database schema
     * @param params          The parameters used in the creation
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(Connection connection, Database desiredDb, CreationParameters params, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param connection A connection to the existing database that shall be modified
     * @param desiredDb  The desired database schema
     * @param params     The parameters used in the creation
     * @return The SQL statements
     */
    public String getAlterTablesSql(Connection connection, Database desiredDb, CreationParameters params) throws DatabaseOperationException;

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param connection      A connection to the existing database that shall be modified
     * @param catalog         The catalog in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param schema          The schema in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param tableTypes      The table types to read from the existing database;
     *                        use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb       The desired database schema
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredDb, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param connection A connection to the existing database that shall be modified
     * @param catalog    The catalog in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param schema     The schema in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param tableTypes The table types to read from the existing database;
     *                   use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb  The desired database schema
     * @return The SQL statements
     */
    public String getAlterTablesSql(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredDb) throws DatabaseOperationException;

    /*
     * Alters the database schema so that it match the given model.
     *
     * @param connection      A connection to the existing database that shall be modified
     * @param catalog         The catalog in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param schema          The schema in the existing database to read (can be a pattern);
     *                        use <code>null</code> for the platform-specific default value
     * @param tableTypes      The table types to read from the existing database;
     *                        use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb       The desired database schema
     * @param params          The parameters used in the creation
     * @param continueOnError Whether to continue with the next sql statement when an error occurred
     */
    public void alterTables(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredDb, CreationParameters params, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for altering the database schema so that it match the given model.
     *
     * @param connection A connection to the existing database that shall be modified
     * @param catalog    The catalog in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param schema     The schema in the existing database to read (can be a pattern);
     *                   use <code>null</code> for the platform-specific default value
     * @param tableTypes The table types to read from the existing database;
     *                   use <code>null</code> or an empty array for the platform-specific default value
     * @param desiredDb  The desired database schema
     * @param params     The parameters used in the creation
     * @return The SQL statements
     */
    public String getAlterTablesSql(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredDb, CreationParameters params) throws DatabaseOperationException;

    /*
     * Drops the specified table and all foreign keys pointing to it.
     * 
     * @param model           The database model
     * @param table           The table to drop
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void dropTable(Database model, Table table, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for dropping the given table and all foreign keys pointing to it.
     * 
     * @param model           The database model
     * @param table           The table to drop
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     * @return The SQL statements
     */
    public String getDropTableSql(Database model, Table table, boolean continueOnError);

    /*
     * Drops the specified table and all foreign keys pointing to it.
     * 
     * @param connection      The connection to the database
     * @param model           The database model
     * @param table           The table to drop
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void dropTable(Connection connection, Database model, Table table, boolean continueOnError) throws DatabaseOperationException; 

    /*
     * Drops the tables defined in the given database.
     * 
     * @param model           The database model
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void dropTables(Database model, boolean continueOnError) throws DatabaseOperationException;

    /*
     * Returns the SQL for dropping the tables defined in the given database.
     * 
     * @param model           The database model
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     * @return The SQL statements
     */
    public String getDropTablesSql(Database model, boolean continueOnError);

    /*
     * Drops the tables defined in the given database.
     * 
     * @param connection      The connection to the database
     * @param model           The database model
     * @param continueOnError Whether to continue executing the sql commands when an error occurred
     */
    public void dropTables(Connection connection, Database model, boolean continueOnError) throws DatabaseOperationException; 

    /*
     * Reads the database model from the live database as specified by the data source set for
     * this platform.
     * 
     * @param name The name of the resulting database; <code>null</code> when the default name (the catalog)
     *             is desired which might be <code>null</code> itself though
     * @return The database model
     * @throws DatabaseOperationException If an error occurred during reading the model
     */
    public Database readModelFromDatabase(String name) throws DatabaseOperationException;

    /*
     * Reads the database model from the live database as specified by the data source set for
     * this platform.
     * 
     * @param name       The name of the resulting database; <code>null</code> when the default name (the catalog)
     *                   is desired which might be <code>null</code> itself though
     * @param catalog    The catalog to access in the database; use <code>null</code> for the default value
     * @param schema     The schema to access in the database; use <code>null</code> for the default value
     * @param tableTypes The table types to process; use <code>null</code> or an empty list for the default ones
     * @return The database model
     * @throws DatabaseOperationException If an error occurred during reading the model
     */
    public Database readModelFromDatabase(String name, String catalog, String schema, String[] tableTypes) throws DatabaseOperationException;

    /*
     * Reads the database model from the live database to which the given connection is pointing.
     * 
     * @param connection The connection to the database
     * @param name       The name of the resulting database; <code>null</code> when the default name (the catalog)
     *                   is desired which might be <code>null</code> itself though
     * @return The database model
     * @throws DatabaseOperationException If an error occurred during reading the model
     */
    public Database readModelFromDatabase(Connection connection, String name) throws DatabaseOperationException;

    /*
     * Reads the database model from the live database to which the given connection is pointing.
     * 
     * @param connection The connection to the database
     * @param name       The name of the resulting database; <code>null</code> when the default name (the catalog)
     *                   is desired which might be <code>null</code> itself though
     * @param catalog    The catalog to access in the database; use <code>null</code> for the default value
     * @param schema     The schema to access in the database; use <code>null</code> for the default value
     * @param tableTypes The table types to process; use <code>null</code> or an empty list for the default ones
     * @return The database model
     * @throws DatabaseOperationException If an error occurred during reading the model
     */
    public Database readModelFromDatabase(Connection connection, String name, String catalog, String schema, String[] tableTypes) throws DatabaseOperationException;
    
    
    public Table readTableFromDatabase(Connection connection, String catalogName, String schemaName, String tablename) throws SQLException;
    
}
