package org.jumpmind.symmetric.ddl.task;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Database;

/**
 * Base class for DdlUtils Ant tasks that operate on a database.
 * 
 * @version $Revision: 289996 $
 * @ant.task ignore="true"
 */
public abstract class DatabaseTaskBase extends Task
{
    /** The log. */
    protected Log _log;

    /** The platform configuration. */
    private PlatformConfiguration _platformConf = new PlatformConfiguration();
    /** The sub tasks to execute. */
    private ArrayList _commands = new ArrayList();
    /** Whether to use simple logging (that the Ant task configures itself via the {@link #_verbosity} setting. */
    private boolean _simpleLogging = true;
    /** The verbosity of the task's debug output. */
    private VerbosityLevel _verbosity = null;

    /**
     * Specifies whether simple logging (configured by the task via the <code>verbosity</code>
     * setting) shall be used, or whether logging is configured outside of the task
     * (e.g. via a log4j properties file).
     * 
     * @param simpleLogging Whether to use simple logging or not
     * @ant.not-required Per default, simple logging is enabled.
     */
    public void setSimpleLogging(boolean simpleLogging)
    {
        _simpleLogging = simpleLogging;
    }

    /**
     * Specifies the verbosity of the task's debug output.
     * 
     * @param level The verbosity level
     * @ant.not-required Default is <code>INFO</code>.
     */
    public void setVerbosity(VerbosityLevel level)
    {
        _verbosity = level;
    }

    /**
     * Returns the database type.
     *
     * @return The database type
     */
    public String getDatabaseType()
    {
        return _platformConf.getDatabaseType();
    }

    /**
     * Specifies the database type. You should only need to set this if DdlUtils is not able to
     * derive the setting from the name of the used jdbc driver or the jdbc connection url.
     * If you have to specify this, please post your jdbc driver and connection url combo
     * to the user mailing list so that DdlUtils can be enhanced to support this combo.<br/>
     * Valid values are currently:<br/><code>axion, cloudscape, db2, derby, firebird, hsqldb, interbase,
     * maxdb, mckoi, mssql, mysql, mysql5, oracle, oracle9, oracle10, postgresql, sapdb, sybase</code>
     * 
     * @param type The database type
     * @ant.not-required Per default, DdlUtils tries to determine the database type via JDBC.
     */
    public void setDatabaseType(String type)
    {
        if ((type != null) && (type.length() > 0))
        {
            _platformConf.setDatabaseType(type);
        }
    }

    /**
     * Returns the data source.
     *
     * @return The data source
     */
    public BasicDataSource getDataSource()
    {
        return _platformConf.getDataSource();
    }

    /**
     * Adds the data source to use for accessing the database.
     * 
     * @param dataSource The data source
     */
    public void addConfiguredDatabase(BasicDataSource dataSource)
    {
        _platformConf.setDataSource(dataSource);
    }

    /**
     * Specifies a pattern that defines which database catalogs to use. For some
     * more info on catalog patterns and JDBC, see
     * <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html">java.sql.DatabaseMetaData</a>.
     * 
     * @param catalogPattern The catalog pattern
     * @ant.not-required Per default no specific catalog is used.
     */
    public void setCatalogPattern(String catalogPattern)
    {
        if ((catalogPattern != null) && (catalogPattern.length() > 0))
        {
            _platformConf.setCatalogPattern(catalogPattern);
        }
    }
    
    /**
     * Specifies a pattern that defines which database schemas to use. For some
     * more info on schema patterns and JDBC, see
     * <a href="http://java.sun.com/j2se/1.4.2/docs/api/java/sql/DatabaseMetaData.html">java.sql.DatabaseMetaData</a>.
     * 
     * @param schemaPattern The schema pattern
     * @ant.not-required Per default no specific schema is used.
     */
    public void setSchemaPattern(String schemaPattern)
    {
        if ((schemaPattern != null) && (schemaPattern.length() > 0))
        {
            _platformConf.setSchemaPattern(schemaPattern);
        }
    }

    /**
     * Determines whether delimited SQL identifiers shall be used (the default).
     *
     * @return <code>true</code> if delimited SQL identifiers shall be used
     */
    public boolean isUseDelimitedSqlIdentifiers()
    {
        return _platformConf.isUseDelimitedSqlIdentifiers();
    }

    /**
     * Specifies whether DdlUtils shall use delimited (quoted) identifiers (such as table and column
     * names). Most databases convert undelimited identifiers to uppercase and ignore the case of
     * identifiers when performing any SQL command. Undelimited identifiers also cannot be reserved
     * words and can only contain alphanumerical characters and the underscore.<br/>
     * These limitations do not exist for delimited identifiers where identifiers have to be enclosed
     * in double quotes. Delimited identifiers can contain unicode characters, and even reserved
     * words can be used as identifiers. Please be aware though, that they always have to enclosed
     * in double quotes, and that the case of the identifier will be important in every SQL command
     * executed against the database.
     *
     * @param useDelimitedSqlIdentifiers <code>true</code> if delimited SQL identifiers shall be used
     * @ant.not-required Default is <code>false</code>.
     */
    public void setUseDelimitedSqlIdentifiers(boolean useDelimitedSqlIdentifiers)
    {
        _platformConf.setUseDelimitedSqlIdentifiers(useDelimitedSqlIdentifiers);
    }

    /**
     * Determines whether a table's foreign keys read from a live database
     * shall be sorted alphabetically. Is <code>false</code> by default.
     *
     * @return <code>true</code> if the foreign keys shall be sorted
     */
    public boolean isSortForeignKeys()
    {
        return _platformConf.isSortForeignKeys();
    }

    /**
     * Specifies whether DdlUtils shall sort the foreign keys of a table read from a live database or
     * leave them in the order in which they are returned by the database/JDBC driver. Note that
     * the sort is case sensitive only if delimited identifier mode is on
     * (<code>useDelimitedSqlIdentifiers</code> is set to <code>true</code>).
     *
     * @param sortForeignKeys <code>true</code> if the foreign keys shall be sorted
     * @ant.not-required Default is <code>false</code>.
     */
    public void setSortForeignKeys(boolean sortForeignKeys)
    {
        _platformConf.setSortForeignKeys(sortForeignKeys);
    }

    /**
     * Determines whether the database shall be shut down after the task has finished.
     *
     * @return <code>true</code> if the database shall be shut down
     */
    public boolean isShutdownDatabase()
    {
        return _platformConf.isShutdownDatabase();
    }

    /**
     * Specifies whether DdlUtils shall shut down the database after the task has finished.
     * This is mostly useful for embedded databases.
     *
     * @param shutdownDatabase <code>true</code> if the database shall be shut down
     * @ant.not-required Default is <code>false</code>.
     */
    public void setShutdownDatabase(boolean shutdownDatabase)
    {
        _platformConf.setShutdownDatabase(shutdownDatabase);
    }

    /**
     * Adds a command.
     * 
     * @param command The command
     */
    protected void addCommand(Command command)
    {
        _commands.add(command);
    }

    /**
     * Determines whether there are commands to perform.
     * 
     * @return <code>true</code> if there are commands
     */
    protected boolean hasCommands()
    {
        return !_commands.isEmpty();
    }

    /**
     * Returns the commands.
     * 
     * @return The commands
     */
    protected Iterator getCommands()
    {
        return _commands.iterator();
    }

    /**
     * Creates the platform configuration.
     * 
     * @return The platform configuration
     */
    protected PlatformConfiguration getPlatformConfiguration()
    {
        return _platformConf;
    }

    /**
     * Creates the platform for the configured database.
     * 
     * @return The platform
     */
    protected Platform getPlatform()
    {
        return _platformConf.getPlatform();
    }

    /**
     * Reads the database model on which the commands will work.
     * 
     * @return The database model
     */
    protected abstract Database readModel();

    /**
     * Initializes the logging.
     */
    private void initLogging()
    {
        // For Ant, we're forcing DdlUtils to do logging via log4j to the console
        Properties props = new Properties();
        String     level = (_verbosity == null ? Level.INFO.toString() : _verbosity.getValue()).toUpperCase();

        props.setProperty("log4j.rootCategory", level + ",A");
        props.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        props.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        props.setProperty("log4j.appender.A.layout.ConversionPattern", "%m%n");
        // we don't want debug logging from Digester/Betwixt
        props.setProperty("log4j.logger.org.apache.commons", "WARN");

        LogManager.resetConfiguration();
        PropertyConfigurator.configure(props);

        _log = LogFactory.getLog(getClass());
    }

    /**
     * Executes the commands.
     * 
     * @param model The database model
     */
    protected void executeCommands(Database model) throws BuildException
    {
        for (Iterator it = getCommands(); it.hasNext();)
        {
            Command cmd = (Command)it.next();

            if (cmd.isRequiringModel() && (model == null))
            {
                throw new BuildException("No database model specified");
            }
            if (cmd instanceof DatabaseCommand)
            {
                ((DatabaseCommand)cmd).setPlatformConfiguration(_platformConf);
            }
            cmd.execute(this, model);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void execute() throws BuildException
    {
        if (_simpleLogging) {
            initLogging();
        }

        if (!hasCommands())
        {
            _log.info("No sub tasks specified, so there is nothing to do.");
            return;
        }

        ClassLoader    sysClassLoader = Thread.currentThread().getContextClassLoader();
        AntClassLoader newClassLoader = new AntClassLoader(getClass().getClassLoader(), true);

        // we're changing the thread classloader so that we can access resources
        // from the classpath used to load this task's class
        Thread.currentThread().setContextClassLoader(newClassLoader);

        try
        {
            executeCommands(readModel());
        }
        finally
        {
            if ((getDataSource() != null) && isShutdownDatabase())
            {
                getPlatform().shutdownDatabase();
            }
            // rollback of our classloader change
            Thread.currentThread().setContextClassLoader(sysClassLoader);
        }
    }
}
