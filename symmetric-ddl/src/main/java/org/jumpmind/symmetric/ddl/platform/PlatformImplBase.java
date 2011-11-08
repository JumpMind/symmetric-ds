package org.jumpmind.symmetric.ddl.platform;

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
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ddl.DatabaseOperationException;
import org.jumpmind.symmetric.ddl.DdlUtilsException;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.util.Jdbc3Utils;
import org.jumpmind.symmetric.ddl.util.JdbcSupport;
import org.jumpmind.symmetric.ddl.util.SqlTokenizer;

/*
 * Base class for platform implementations.
 * 
 * @version $Revision: 231110 $
 */
public abstract class PlatformImplBase extends JdbcSupport implements Platform
{
    /* The default name for models read from the database, if no name as given.*/
    protected static final String MODEL_DEFAULT_NAME = "default";

    /* The log for this platform. */
    private final Log _log = LogFactory.getLog(getClass());

    /* The platform info. */
    private PlatformInfo _info = new PlatformInfo();
    /* The sql builder for this platform. */
    private SqlBuilder _builder;
    /* The model reader for this platform. */
    private JdbcModelReader _modelReader;
    /* Whether script mode is on. */
    private boolean _scriptModeOn;
    /* Whether SQL comments are generated or not. */
    private boolean _sqlCommentsOn = false;
    /* Whether delimited identifiers are used or not. */
    private boolean _delimitedIdentifierModeOn;
    /* Whether identity override is enabled. */
    private boolean _identityOverrideOn;
    /* Whether read foreign keys shall be sorted alphabetically. */
    private boolean _foreignKeysSorted;

    /*
     * {@inheritDoc}
     */
    public SqlBuilder getSqlBuilder()
    {
        return _builder;
    }

    /*
     * Sets the sql builder for this platform.
     * 
     * @param builder The sql builder
     */
    protected void setSqlBuilder(SqlBuilder builder)
    {
        _builder = builder;
    }

    /*
     * {@inheritDoc}
     */
    public JdbcModelReader getModelReader()
    {
        if (_modelReader == null)
        {
            _modelReader = new JdbcModelReader(this);
        }
        return _modelReader;
    }

    /*
     * Sets the model reader for this platform.
     * 
     * @param modelReader The model reader
     */
    protected void setModelReader(JdbcModelReader modelReader)
    {
        _modelReader = modelReader;
    }

    /*
     * {@inheritDoc}
     */
    public PlatformInfo getPlatformInfo()
    {
        return _info;
    }

    /*
     * {@inheritDoc}
     */
    public boolean isScriptModeOn()
    {
        return _scriptModeOn;
    }

    /*
     * {@inheritDoc}
     */
    public void setScriptModeOn(boolean scriptModeOn)
    {
        _scriptModeOn = scriptModeOn;
    }

    /*
     * {@inheritDoc}
     */
    public boolean isSqlCommentsOn()
    {
        return _sqlCommentsOn;
    }

    /*
     * {@inheritDoc}
     */
    public void setSqlCommentsOn(boolean sqlCommentsOn)
    {
        if (!getPlatformInfo().isSqlCommentsSupported() && sqlCommentsOn)
        {
            throw new DdlUtilsException("Platform " + getName() + " does not support SQL comments");
        }
        _sqlCommentsOn = sqlCommentsOn;
    }

    /*
     * {@inheritDoc}
     */
    public boolean isDelimitedIdentifierModeOn()
    {
        return _delimitedIdentifierModeOn;
    }

    /*
     * {@inheritDoc}
     */
    public void setDelimitedIdentifierModeOn(boolean delimitedIdentifierModeOn)
    {
        if (!getPlatformInfo().isDelimitedIdentifiersSupported() && delimitedIdentifierModeOn)
        {
            throw new DdlUtilsException("Platform " + getName() + " does not support delimited identifier");
        }
        _delimitedIdentifierModeOn = delimitedIdentifierModeOn;
    }

    /*
     * {@inheritDoc}
     */
    public boolean isIdentityOverrideOn()
    {
        return _identityOverrideOn;
    }

    /*
     * {@inheritDoc}
     */
    public void setIdentityOverrideOn(boolean identityOverrideOn)
    {
        _identityOverrideOn = identityOverrideOn;
    }

    /*
     * {@inheritDoc}
     */
    public boolean isForeignKeysSorted()
    {
        return _foreignKeysSorted;
    }

    /*
     * {@inheritDoc}
     */
    public void setForeignKeysSorted(boolean foreignKeysSorted)
    {
        _foreignKeysSorted = foreignKeysSorted;
    }

    /*
     * Returns the log for this platform.
     * 
     * @return The log
     */
    protected Log getLog()
    {
        return _log;
    }

    /*
     * Logs any warnings associated to the given connection. Note that the connection needs
     * to be open for this.
     * 
     * @param connection The open connection
     */
    protected void logWarnings(Connection connection) throws SQLException
    {
        SQLWarning warning = connection.getWarnings();

        while (warning != null)
        {
            getLog().warn(warning.getLocalizedMessage(), warning.getCause());
            warning = warning.getNextWarning();
        }
    }

    /*
     * {@inheritDoc}
     */
    public int evaluateBatch(String sql, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            return evaluateBatch(connection, sql, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public int evaluateBatch(Connection connection, String sql, boolean continueOnError) throws DatabaseOperationException
    {
        Statement statement    = null;
        int       errors       = 0;
        int       commandCount = 0;

        // we tokenize the SQL along the delimiters, and we also make sure that only delimiters
        // at the end of a line or the end of the string are used (row mode)
        try
        {
            statement = connection.createStatement();

            SqlTokenizer tokenizer = new SqlTokenizer(sql);

            while (tokenizer.hasMoreStatements())
            {
                String command = tokenizer.getNextStatement();
                
                // ignore whitespace
                command = command.trim();
                if (command.length() == 0)
                {
                    continue;
                }
                
                commandCount++;
                
                if (_log.isDebugEnabled())
                {
                    _log.debug("Executing SQL: " + command);
                }
                try
                {
                    int results = statement.executeUpdate(command);

                    if (_log.isDebugEnabled())
                    {
                        _log.debug("After execution, " + results + " row(s) have been changed");
                    }
                }
                catch (SQLException ex)
                {
                    if (continueOnError)
                    {
                        // Since the user deciced to ignore this error, we log the error
                        // on level warn, and the exception itself on level debug
                        _log.warn("SQL Command " + command + " failed with: " + ex.getMessage());
                        if (_log.isDebugEnabled())
                        {
                            _log.debug(ex);
                        }
                        errors++;
                    }
                    else
                    {
                        throw new DatabaseOperationException("Error while executing SQL "+command, ex);
                    }
                }

                // lets display any warnings
                SQLWarning warning = connection.getWarnings();

                while (warning != null)
                {
                    _log.warn(warning.toString());
                    warning = warning.getNextWarning();
                }
                connection.clearWarnings();
            }
            _log.info("Executed "+ commandCount + " SQL command(s) with " + errors + " error(s)");
        }
        catch (SQLException ex)
        {
            throw new DatabaseOperationException("Error while executing SQL", ex);
        }
        finally
        {
            closeStatement(statement);
        }

        return errors;
    }

    /*
     * {@inheritDoc}
     */
    public void shutdownDatabase() throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            shutdownDatabase(connection);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void shutdownDatabase(Connection connection) throws DatabaseOperationException
    {
        // Per default do nothing as most databases don't need this
    }

    /*
     * {@inheritDoc}
     */
    public void createDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password, Map parameters) throws DatabaseOperationException, UnsupportedOperationException
    {
        throw new UnsupportedOperationException("Database creation is not supported for the database platform "+getName());
    }

    /*
     * {@inheritDoc}
     */
    public void dropDatabase(String jdbcDriverClassName, String connectionUrl, String username, String password) throws DatabaseOperationException, UnsupportedOperationException
    {
        throw new UnsupportedOperationException("Database deletion is not supported for the database platform "+getName());
    }

    /*
     * {@inheritDoc}
     */
    public void createTables(Database model, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            createTables(connection, model, dropTablesFirst, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void createTables(Connection connection, Database model, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException
    {
        String sql = getCreateTablesSql(model, dropTablesFirst, continueOnError);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public String getCreateTablesSql(Database model, boolean dropTablesFirst, boolean continueOnError)
    {
        String sql = null;

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().createTables(model, dropTablesFirst);
            sql = buffer.toString();
        }
        catch (IOException e)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */
    public void createTables(Database model, CreationParameters params, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            createTables(connection, model, params, dropTablesFirst, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void createTables(Connection connection, Database model, CreationParameters params, boolean dropTablesFirst, boolean continueOnError) throws DatabaseOperationException
    {
        String sql = getCreateTablesSql(model, params, dropTablesFirst, continueOnError);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public String getCreateTablesSql(Database model, CreationParameters params, boolean dropTablesFirst, boolean continueOnError)
    {
        String sql = null;

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().createTables(model, params, dropTablesFirst);
            sql = buffer.toString();
        }
        catch (IOException e)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(Database desiredDb, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            alterTables(connection, desiredDb, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(Database desiredDb) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            return getAlterTablesSql(connection, desiredDb);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(Database desiredDb, CreationParameters params, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            alterTables(connection, desiredDb, params, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(Database desiredDb, CreationParameters params) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            return getAlterTablesSql(connection, desiredDb, params);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(Connection connection, Database desiredModel, boolean continueOnError) throws DatabaseOperationException
    {
        String sql = getAlterTablesSql(connection, desiredModel);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(Connection connection, Database desiredModel) throws DatabaseOperationException
    {
        String   sql          = null;
        Database currentModel = readModelFromDatabase(connection, desiredModel.getName());

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().alterDatabase(currentModel, desiredModel, null);
            sql = buffer.toString();
        }
        catch (IOException ex)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(Connection connection, Database desiredModel, CreationParameters params, boolean continueOnError) throws DatabaseOperationException
    {
        String sql = getAlterTablesSql(connection, desiredModel, params);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(Connection connection, Database desiredModel, CreationParameters params) throws DatabaseOperationException
    {
        String   sql          = null;
        Database currentModel = readModelFromDatabase(connection, desiredModel.getName());

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().alterDatabase(currentModel, desiredModel, params);
            sql = buffer.toString();
        }
        catch (IOException ex)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(String catalog, String schema, String[] tableTypes, Database desiredModel, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            alterTables(connection, catalog, schema, tableTypes, desiredModel, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(String catalog, String schema, String[] tableTypes, Database desiredModel) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            return getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(String catalog, String schema, String[] tableTypes, Database desiredModel, CreationParameters params, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            alterTables(connection, catalog, schema, tableTypes, desiredModel, params, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(String catalog, String schema, String[] tableTypes, Database desiredModel, CreationParameters params) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            return getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel, params);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredModel, boolean continueOnError) throws DatabaseOperationException
    {
        String sql = getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredModel) throws DatabaseOperationException
    {
        String   sql          = null;
        Database currentModel = readModelFromDatabase(connection, desiredModel.getName(), catalog, schema, tableTypes);

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().alterDatabase(currentModel, desiredModel, null);
            sql = buffer.toString();
        }
        catch (IOException ex)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */
    public void alterTables(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredModel, CreationParameters params, boolean continueOnError) throws DatabaseOperationException
    {
        String sql = getAlterTablesSql(connection, catalog, schema, tableTypes, desiredModel, params);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public String getAlterTablesSql(Connection connection, String catalog, String schema, String[] tableTypes, Database desiredModel, CreationParameters params) throws DatabaseOperationException
    {
        String   sql          = null;
        Database currentModel = readModelFromDatabase(connection, desiredModel.getName(), catalog, schema, tableTypes);

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().alterDatabase(currentModel, desiredModel, params);
            sql = buffer.toString();
        }
        catch (IOException ex)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */
    public void dropTable(Connection connection, Database model, Table table, boolean continueOnError) throws DatabaseOperationException
    {
        String sql = getDropTableSql(model, table, continueOnError);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public void dropTable(Database model, Table table, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            dropTable(connection, model, table, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public String getDropTableSql(Database model, Table table, boolean continueOnError)
    {
        String sql = null;

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().dropTable(model, table);
            sql = buffer.toString();
        }
        catch (IOException e)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */
    public void dropTables(Database model, boolean continueOnError) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            dropTables(connection, model, continueOnError);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public void dropTables(Connection connection, Database model, boolean continueOnError) throws DatabaseOperationException 
    {
        String sql = getDropTablesSql(model, continueOnError);

        evaluateBatch(connection, sql, continueOnError);
    }

    /*
     * {@inheritDoc}
     */
    public String getDropTablesSql(Database model, boolean continueOnError) 
    {
        String sql = null;

        try
        {
            StringWriter buffer = new StringWriter();

            getSqlBuilder().setWriter(buffer);
            getSqlBuilder().dropTables(model);
            sql = buffer.toString();
        }
        catch (IOException e)
        {
            // won't happen because we're using a string writer
        }
        return sql;
    }

    /*
     * {@inheritDoc}
     */    
    public Database readModelFromDatabase(String name) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            return readModelFromDatabase(connection, name);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */    
    public Database readModelFromDatabase(Connection connection, String name) throws DatabaseOperationException
    {
        try
        {
            Database model = getModelReader().getDatabase(connection, name);

            postprocessModelFromDatabase(model);
            return model;
        }
        catch (SQLException ex)
        {
            throw new DatabaseOperationException(ex);
        }
    }

    /*
     * {@inheritDoc}
     */
    public Database readModelFromDatabase(String name, String catalog, String schema, String[] tableTypes) throws DatabaseOperationException
    {
        Connection connection = borrowConnection();

        try
        {
            return readModelFromDatabase(connection, name, catalog, schema, tableTypes);
        }
        finally
        {
            returnConnection(connection);
        }
    }

    /*
     * {@inheritDoc}
     */
    public Database readModelFromDatabase(Connection connection, String name, String catalog, String schema, String[] tableTypes) throws DatabaseOperationException
    {
        try
        {
            JdbcModelReader reader = getModelReader();
            Database        model  = reader.getDatabase(connection, name, catalog, schema, tableTypes);

            postprocessModelFromDatabase(model);
            if ((model.getName() == null) || (model.getName().length() == 0))
            {
                model.setName(MODEL_DEFAULT_NAME);
            }
            return model;
        }
        catch (SQLException ex)
        {
            throw new DatabaseOperationException(ex);
        }
    }
    
    public Table readTableFromDatabase(Connection connection, String catalogName,
            String schemaName, String tablename) throws SQLException {
         return postprocessTableFromDatabase(_modelReader.readTable(connection, catalogName, schemaName, tablename));
    }

    /*
     * Allows the platform to postprocess the model just read from the database.
     * 
     * @param model The model
     */
    protected void postprocessModelFromDatabase(Database model)
    {
        // Default values for CHAR/VARCHAR/LONGVARCHAR columns have quotation marks
        // around them which we'll remove now
        for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++)
        {
            postprocessTableFromDatabase(model.getTable(tableIdx));
        }
    }
    
    protected Table postprocessTableFromDatabase(Table table) {
        if (table != null) {
            for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++) {
                Column column = table.getColumn(columnIdx);

                if (TypeMap.isTextType(column.getTypeCode())
                        || TypeMap.isDateTimeType(column.getTypeCode())) {
                    String defaultValue = column.getDefaultValue();

                    if ((defaultValue != null) && (defaultValue.length() >= 2)
                            && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                        defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                        column.setDefaultValue(defaultValue);
                    }
                }
            }
        }
        return table;
    }
    
    /*
     * This is the core method to set the parameter of a prepared statement to a given value.
     * The primary purpose of this method is to call the appropriate method on the statement,
     * and to give database-specific implementations the ability to change this behavior.
     * 
     * @param statement The statement
     * @param sqlIndex  The parameter index
     * @param typeCode  The JDBC type code
     * @param value     The value
     * @throws SQLException If an error occurred while setting the parameter value
     */
    protected void setStatementParameterValue(PreparedStatement statement, int sqlIndex, int typeCode, Object value) throws SQLException
    {
        if (value == null)
        {
            statement.setNull(sqlIndex, typeCode);
        }
        else if (value instanceof String)
        {
            statement.setString(sqlIndex, (String)value);
        }
        else if (value instanceof byte[])
        {
            statement.setBytes(sqlIndex, (byte[])value);
        }
        else if (value instanceof Boolean)
        {
            statement.setBoolean(sqlIndex, ((Boolean)value).booleanValue());
        }
        else if (value instanceof Byte)
        {
            statement.setByte(sqlIndex, ((Byte)value).byteValue());
        }
        else if (value instanceof Short)
        {
            statement.setShort(sqlIndex, ((Short)value).shortValue());
        }
        else if (value instanceof Integer)
        {
            statement.setInt(sqlIndex, ((Integer)value).intValue());
        }
        else if (value instanceof Long)
        {
            statement.setLong(sqlIndex, ((Long)value).longValue());
        }
        else if (value instanceof BigDecimal)
        {
            // setObject assumes a scale of 0, so we rather use the typed setter
            statement.setBigDecimal(sqlIndex, (BigDecimal)value);
        }
        else if (value instanceof Float)
        {
            statement.setFloat(sqlIndex, ((Float)value).floatValue());
        }
        else if (value instanceof Double)
        {
            statement.setDouble(sqlIndex, ((Double)value).doubleValue());
        }
        else
        {
            statement.setObject(sqlIndex, value, typeCode);
        }
    }

    /*
     * Helper method esp. for the {@link ModelBasedResultSetIterator} class that retrieves
     * the value for a column from the given result set. If a table was specified,
     * and it contains the column, then the jdbc type defined for the column is used for extracting
     * the value, otherwise the object directly retrieved from the result set is returned.<br/>
     * The method is defined here rather than in the {@link ModelBasedResultSetIterator} class
     * so that concrete platforms can modify its behavior.
     * 
     * @param resultSet  The result set
     * @param columnName The name of the column
     * @param table      The table
     * @return The value
     */
    protected Object getObjectFromResultSet(ResultSet resultSet, String columnName, Table table) throws SQLException
    {
        Column column = (table == null ? null : table.findColumn(columnName, isDelimitedIdentifierModeOn()));
        Object value  = null;

        if (column != null)
        {
            int originalJdbcType = column.getTypeCode();
            int targetJdbcType   = getPlatformInfo().getTargetJdbcType(originalJdbcType);
            int jdbcType         = originalJdbcType;

            // in general we're trying to retrieve the value using the original type
            // but sometimes we also need the target type:
            if ((originalJdbcType == Types.BLOB) && (targetJdbcType != Types.BLOB))
            {
                // we should not use the Blob interface if the database doesn't map to this type 
                jdbcType = targetJdbcType;
            }
            if ((originalJdbcType == Types.CLOB) && (targetJdbcType != Types.CLOB))
            {
                // we should not use the Clob interface if the database doesn't map to this type 
                jdbcType = targetJdbcType;
            }
            value = extractColumnValue(resultSet, columnName, 0, jdbcType);
        }
        else
        {
            value = resultSet.getObject(columnName);
        }
        return resultSet.wasNull() ? null : value;
    }

    /*
     * Helper method for retrieving the value for a column from the given result set
     * using the type code of the column.
     * 
     * @param resultSet The result set
     * @param column    The column
     * @param idx       The value's index in the result set (starting from 1) 
     * @return The value
     */
    protected Object getObjectFromResultSet(ResultSet resultSet, Column column, int idx) throws SQLException
    {
        int    originalJdbcType = column.getTypeCode();
        int    targetJdbcType   = getPlatformInfo().getTargetJdbcType(originalJdbcType);
        int    jdbcType         = originalJdbcType;
        Object value            = null;

        // in general we're trying to retrieve the value using the original type
        // but sometimes we also need the target type:
        if ((originalJdbcType == Types.BLOB) && (targetJdbcType != Types.BLOB))
        {
            // we should not use the Blob interface if the database doesn't map to this type 
            jdbcType = targetJdbcType;
        }
        if ((originalJdbcType == Types.CLOB) && (targetJdbcType != Types.CLOB))
        {
            // we should not use the Clob interface if the database doesn't map to this type 
            jdbcType = targetJdbcType;
        }
        value = extractColumnValue(resultSet, null, idx, jdbcType);
        return resultSet.wasNull() ? null : value;
    }

    /*
     * This is the core method to retrieve a value for a column from a result set. Its  primary
     * purpose is to call the appropriate method on the result set, and to provide an extension
     * point where database-specific implementations can change this behavior.
     * 
     * @param resultSet  The result set to extract the value from
     * @param columnName The name of the column; can be <code>null</code> in which case the
     *                   <code>columnIdx</code> will be used instead
     * @param columnIdx  The index of the column's value in the result set; is only used if
     *                   <code>columnName</code> is <code>null</code>
     * @param jdbcType   The jdbc type to extract
     * @return The value
     * @throws SQLException If an error occurred while accessing the result set
     */
    protected Object extractColumnValue(ResultSet resultSet, String columnName, int columnIdx, int jdbcType) throws SQLException
    {
        boolean useIdx = (columnName == null);
        Object  value;

        switch (jdbcType)
        {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                value = useIdx ? resultSet.getString(columnIdx) : resultSet.getString(columnName);
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
                value = useIdx ? resultSet.getBigDecimal(columnIdx) : resultSet.getBigDecimal(columnName);
                break;
            case Types.BIT:
                value = new Boolean(useIdx ? resultSet.getBoolean(columnIdx) : resultSet.getBoolean(columnName));
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                value = new Integer(useIdx ? resultSet.getInt(columnIdx) : resultSet.getInt(columnName));
                break;
            case Types.BIGINT:
                value = new Long(useIdx ? resultSet.getLong(columnIdx) : resultSet.getLong(columnName));
                break;
            case Types.REAL:
                value = new Float(useIdx ? resultSet.getFloat(columnIdx) : resultSet.getFloat(columnName));
                break;
            case Types.FLOAT:
            case Types.DOUBLE:
                value = new Double(useIdx ? resultSet.getDouble(columnIdx) : resultSet.getDouble(columnName));
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                value = useIdx ? resultSet.getBytes(columnIdx) : resultSet.getBytes(columnName);
                break;
            case Types.DATE:
                value = useIdx ? resultSet.getDate(columnIdx) : resultSet.getDate(columnName);
                break;
            case Types.TIME:
                value = useIdx ? resultSet.getTime(columnIdx) : resultSet.getTime(columnName);
                break;
            case Types.TIMESTAMP:
                value = useIdx ? resultSet.getTimestamp(columnIdx) : resultSet.getTimestamp(columnName);
                break;
            case Types.CLOB:
                Clob clob = useIdx ? resultSet.getClob(columnIdx) : resultSet.getClob(columnName);

                if (clob == null)
                {
                    value = null;
                }
                else
                {
                    long length = clob.length();
    
                    if (length > Integer.MAX_VALUE)
                    {
                        value = clob;
                    }
                    else if (length == 0)
                    {
                        // the javadoc is not clear about whether Clob.getSubString
                        // can be used with a substring length of 0
                        // thus we do the safe thing and handle it ourselves
                        value = "";
                    }
                    else
                    {
                        value = clob.getSubString(1l, (int)length);
                    }
                }
                break;
            case Types.BLOB:
                Blob blob = useIdx ? resultSet.getBlob(columnIdx) : resultSet.getBlob(columnName);

                if (blob == null)
                {
                    value = null;
                }
                else
                {
                    long length = blob.length();
    
                    if (length > Integer.MAX_VALUE)
                    {
                        value = blob;
                    }
                    else if (length == 0)
                    {
                        // the javadoc is not clear about whether Blob.getBytes
                        // can be used with for 0 bytes to be copied
                        // thus we do the safe thing and handle it ourselves
                        value = new byte[0];
                    }
                    else
                    {
                        value = blob.getBytes(1l, (int)length);
                    }
                }
                break;
            case Types.ARRAY:
                value = useIdx ? resultSet.getArray(columnIdx) : resultSet.getArray(columnName);
                break;
            case Types.REF:
                value = useIdx ? resultSet.getRef(columnIdx) : resultSet.getRef(columnName);
                break;
            default:
                // special handling for Java 1.4/JDBC 3 types
                if (Jdbc3Utils.supportsJava14JdbcTypes() &&
                    (jdbcType == Jdbc3Utils.determineBooleanTypeCode()))
                {
                    value = new Boolean(useIdx ? resultSet.getBoolean(columnIdx) : resultSet.getBoolean(columnName));
                }
                else
                {
                    value = useIdx ? resultSet.getObject(columnIdx) : resultSet.getObject(columnName);
                }
                break;
        }
        return resultSet.wasNull() ? null : value;
    }

}
