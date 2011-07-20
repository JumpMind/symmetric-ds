package org.jumpmind.symmetric.ddl.util;

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
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ddl.DatabaseOperationException;

/**
 * JdbcSupport is an abstract base class for objects which need to 
 * perform JDBC operations. It contains a number of useful methods 
 * for implementation inheritence..
 *
 * @version $Revision: 463757 $
 */
public abstract class JdbcSupport
{
    /** The Log to which logging calls will be made. */
    private final Log _log = LogFactory.getLog(JdbcSupport.class);
    /** The data source. */
    private DataSource _dataSource;
    /** The username for accessing the database. */
    private String _username;
    /** The password for accessing the database. */
    private String _password;
    /** The names of the currently borrowed connections (for debugging). */
    private HashSet _openConnectionNames = new HashSet();

    // Properties
    //-------------------------------------------------------------------------                
    
    /**
     * Returns the data source used for communicating with the database.
     * 
     * @return The data source
     */
    public DataSource getDataSource()
    {
        return _dataSource;
    }

    /**
     * Sets the DataSource used for communicating with the database.
     * 
     * @param dataSource The data source
     */
    public void setDataSource(DataSource dataSource)
    {
        _dataSource = dataSource;
    }


    /**
     * Returns the username used to access the database.
     * 
     * @return The username
     */
    public String getUsername()
    {
        return _username;
    }

    /**
     * Sets the username to be used to access the database.
     * 
     * @param username The username
     */
    public void setUsername(String username)
    {
        _username = username;
    }

    /**
     * Returns the password used to access the database.
     * 
     * @return The password
     */
    public String getPassword()
    {
        return _password;
    }

    /**
     * Sets the password to be used to access the database.
     * 
     * @param password The password
     */
    public void setPassword(String password)
    {
        _password = password;
    }

    // Implementation methods    
    //-------------------------------------------------------------------------                

    /**
     * Returns a (new) JDBC connection from the data source.
     * 
     * @return The connection
     */
    public Connection borrowConnection() throws DatabaseOperationException
    {
        try
        {
            Connection connection = null;

            if (_username == null)
            {
                connection = getDataSource().getConnection();
            }
            else
            {
                connection = getDataSource().getConnection(_username, _password);
            }
            if (_log.isDebugEnabled())
            {
                String connName = connection.toString();

                _log.debug("Borrowed connection "+connName+" from data source");
                _openConnectionNames.add(connName);
            }
            return connection;
        }
        catch (SQLException ex)
        {
            throw new DatabaseOperationException("Could not get a connection from the datasource", ex);
        }
    }
    
    /**
     * Closes the given JDBC connection (returns it back to the pool if the datasource is poolable).
     * 
     * @param connection The connection
     */
    public void returnConnection(Connection connection)
    {
        try
        {
            if ((connection != null) && !connection.isClosed())
            {
                if (_log.isDebugEnabled())
                {
                    String connName = connection.toString();

                    _openConnectionNames.remove(connName);

                    StringBuffer logMsg = new StringBuffer();

                    logMsg.append("Returning connection ");
                    logMsg.append(connName);
                    logMsg.append(" to data source.\nRemaining connections:");
                    if (_openConnectionNames.isEmpty())
                    {
                        logMsg.append(" None");
                    }
                    else
                    {
                        for (Iterator it = _openConnectionNames.iterator(); it.hasNext();)
                        {
                          logMsg.append("\n    ");
                          logMsg.append(it.next().toString());
                        }
                    }
                    _log.debug(logMsg.toString());
                }
                connection.close();
            }
        }
        catch (Exception e)
        {
            _log.warn("Caught exception while returning connection to pool", e);
        }
    }

    /**
     * Closes the given statement (which also closes all result sets for this statement) and the
     * connection it belongs to.
     * 
     * @param statement The statement
     */
    public void closeStatement(Statement statement)
    {
        if (statement != null)
        {
            try
            {
                statement.close();
            }
            catch (Exception e)
            {
                _log.debug("Ignoring exception that occurred while closing statement", e);
            }
        }
    }
}
