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

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.DynaBean;
import org.apache.commons.beanutils.DynaProperty;
import org.apache.commons.dbcp.BasicDataSource;
import org.jumpmind.symmetric.ddl.DatabaseOperationException;
import org.jumpmind.symmetric.ddl.PlatformUtils;
import org.jumpmind.symmetric.ddl.io.DataReader;
import org.jumpmind.symmetric.ddl.io.DataToDatabaseSink;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.platform.CreationParameters;

/**
 * Base class for database writer tests.
 * 
 * @version $Revision: 289996 $
 */
public abstract class TestDatabaseWriterBase extends TestPlatformBase
{
    /** The name of the property that specifies properties file with the settings for the connection to test against. */
    public static final String JDBC_PROPERTIES_PROPERTY = "jdbc.properties.file";
    /** The prefix for properties of the datasource. */
    public static final String DATASOURCE_PROPERTY_PREFIX = "datasource.";
    /** The prefix for properties for ddlutils. */
    public static final String DDLUTILS_PROPERTY_PREFIX = "ddlutils.";
    /** The property for specifying the platform. */
    public static final String DDLUTILS_PLATFORM_PROPERTY = DDLUTILS_PROPERTY_PREFIX + "platform";
    /** The property specifying the catalog for the tests. */
    public static final String DDLUTILS_CATALOG_PROPERTY = DDLUTILS_PROPERTY_PREFIX + "catalog";
    /** The property specifying the schema for the tests. */
    public static final String DDLUTILS_SCHEMA_PROPERTY = DDLUTILS_PROPERTY_PREFIX + "schema";
    /** The prefix for table creation properties. */
    public static final String DDLUTILS_TABLE_CREATION_PREFIX = DDLUTILS_PROPERTY_PREFIX + "tableCreation.";

    /** The test properties as defined by an external properties file. */
    private static Properties _testProps;
    /** The data source to test against. */
    private static DataSource _dataSource;
    /** The database name. */
    private static String _databaseName;
    /** The database model. */
    private Database _model;

    /**
     * Creates a new test case instance.
     */
    public TestDatabaseWriterBase()
    {
        super();
        init();
    }

    /**
     * Returns the test properties.
     * 
     * @return The properties
     */
    protected Properties getTestProperties()
    {
    	if (_testProps == null)
    	{
    		String propFile = System.getProperty(JDBC_PROPERTIES_PROPERTY);
	
	        if (propFile == null)
	        {
	        	throw new RuntimeException("Please specify the properties file via the jdbc.properties.file environment variable");
	        }
	        try
	        {
	            InputStream propStream = getClass().getResourceAsStream(propFile);
	
	            if (propStream == null)
	            {
	                propStream = new FileInputStream(propFile);
	            }
	            Properties props = new Properties();

	            props.load(propStream);
	            _testProps = props;
	        }
	        catch (Exception ex)
	        {
	        	throw new RuntimeException(ex);
	        }
    	}
    	return _testProps;
    }
    
    /**
     * Initializes the test datasource and the platform.
     */
    private void init()
    {
        // the data source won't change during the tests, hence
        // it is static and needs to be initialized only once
        if (_dataSource != null)
        {
            return;
        }

        Properties props = getTestProperties();

        try
        {
            String dataSourceClass = props.getProperty(DATASOURCE_PROPERTY_PREFIX + "class", BasicDataSource.class.getName());

            _dataSource = (DataSource)Class.forName(dataSourceClass).newInstance();

            for (Iterator it = props.entrySet().iterator(); it.hasNext();)
            {
                Map.Entry entry    = (Map.Entry)it.next();
                String    propName = (String)entry.getKey();

                if (propName.startsWith(DATASOURCE_PROPERTY_PREFIX) && !propName.equals(DATASOURCE_PROPERTY_PREFIX +"class"))
                {
                    BeanUtils.setProperty(_dataSource,
                                          propName.substring(DATASOURCE_PROPERTY_PREFIX.length()),
                                          entry.getValue());
                }
            }
        }
        catch (Exception ex)
        {
            throw new DatabaseOperationException(ex);
        }

        _databaseName = props.getProperty(DDLUTILS_PLATFORM_PROPERTY);
        if (_databaseName == null)
        {
            // property not set, then try to determine
            _databaseName = new PlatformUtils().determineDatabaseType(_dataSource);
            if (_databaseName == null)
            {
                throw new DatabaseOperationException("Could not determine platform from datasource, please specify it in the jdbc.properties via the ddlutils.platform property");
            }
        }
    }

    /**
     * Returns the test table creation parameters for the given model.
     * 
     * @param model The model
     * @return The creation parameters
     */
    protected CreationParameters getTableCreationParameters(Database model)
    {
        CreationParameters params = new CreationParameters();

        for (Iterator entryIt = _testProps.entrySet().iterator(); entryIt.hasNext();)
        {
            Map.Entry entry = (Map.Entry)entryIt.next();
            String    name  = (String)entry.getKey();
            String    value = (String)entry.getValue();

            if (name.startsWith(DDLUTILS_TABLE_CREATION_PREFIX))
            {
                name = name.substring(DDLUTILS_TABLE_CREATION_PREFIX.length());
                for (int tableIdx = 0; tableIdx < model.getTableCount(); tableIdx++)
                {
                    params.addParameter(model.getTable(tableIdx), name, value);
                }
            }
        }
        return params;
    }

    /**
     * Returns the data source.
     * 
     * @return The data source
     */
    protected DataSource getDataSource()
    {
        return _dataSource;
    }

    /**
     * {@inheritDoc}
     */
    protected String getDatabaseName()
    {
        return _databaseName;
    }

    /**
     * Returns the database model.
     * 
     * @return The model
     */
    protected Database getModel()
    {
        return _model;
    }

    /**
     * {@inheritDoc}
     */
    protected void setUp() throws Exception
    {
        super.setUp();
        getPlatform().setDataSource(getDataSource());
    }

    /**
     * {@inheritDoc}
     */
    protected void tearDown() throws Exception
    {
        if (_model != null)
        {
            dropDatabase();
            _model = null;
        }
        super.tearDown();
    }

    /**
     * Creates a new database from the given XML database schema.
     * 
     * @param schemaXml The XML database schema
     * @return The parsed database model
     */
    protected Database createDatabase(String schemaXml) throws DatabaseOperationException
    {
    	Database model = parseDatabaseFromString(schemaXml);

    	createDatabase(model);
    	return model;
    }

    /**
     * Creates a new database from the given model.
     * 
     * @param model The model
     */
    protected void createDatabase(Database model) throws DatabaseOperationException
    {
        try
        {
            _model = model;

            getPlatform().setSqlCommentsOn(false);
            getPlatform().createTables(_model, getTableCreationParameters(_model), false, false);
        }
        catch (Exception ex)
        {
            throw new DatabaseOperationException(ex);
        }
    }

    /**
     * Alters the database to match the given model.
     * 
     * @param schemaXml The model XML
     * @return The model object
     */
    protected Database alterDatabase(String schemaXml) throws DatabaseOperationException
    {
        Database model = parseDatabaseFromString(schemaXml);

        alterDatabase(model);
        return model;
    }

    /**
     * Alters the database to match the given model.
     * 
     * @param model The model
     */
    protected void alterDatabase(Database model) throws DatabaseOperationException
    {
        Properties props   = getTestProperties();
        String     catalog = props.getProperty(DDLUTILS_CATALOG_PROPERTY);
        String     schema  = props.getProperty(DDLUTILS_SCHEMA_PROPERTY);

        try
        {
            _model = model;
            _model.resetDynaClassCache();

            getPlatform().setSqlCommentsOn(false);
            getPlatform().alterTables(catalog, schema, null, _model, getTableCreationParameters(_model), false);
        }
        catch (Exception ex)
        {
            throw new DatabaseOperationException(ex);
        }
    }

    /**
     * Inserts data into the database.
     * 
     * @param dataXml The data xml
     * @return The database
     */
    protected Database insertData(String dataXml) throws DatabaseOperationException
    {
        try
        {
            DataReader dataReader = new DataReader();

            dataReader.setModel(_model);
            dataReader.setSink(new DataToDatabaseSink(getPlatform(), _model));
            dataReader.getSink().start();
            dataReader.parse(new StringReader(dataXml));
            dataReader.getSink().end();
            return _model;
        }
        catch (Exception ex)
        {
            throw new DatabaseOperationException(ex);
        }
    }

    /**
     * Drops the tables defined in the database model.
     */
    protected void dropDatabase() throws DatabaseOperationException
    {
        getPlatform().dropTables(_model, true);
    }

    /**
     * Reads the database model from a live database.
     * 
     * @param databaseName The name of the resulting database
     * @return The model
     */
    protected Database readModelFromDatabase(String databaseName)
    {
    	Properties props   = getTestProperties();
        String     catalog = props.getProperty(DDLUTILS_CATALOG_PROPERTY);
        String     schema  = props.getProperty(DDLUTILS_SCHEMA_PROPERTY);

    	return getPlatform().readModelFromDatabase(databaseName, catalog, schema, null);
    }
    
    /**
     * Returns the SQL for altering the live database so that it matches the given model.
     * 
     * @param desiredModel The desired model
     * @return The alteration SQL
     */
    protected String getAlterTablesSql(Database desiredModel)
    {
    	Properties props   = getTestProperties();
        String     catalog = props.getProperty(DDLUTILS_CATALOG_PROPERTY);
        String     schema  = props.getProperty(DDLUTILS_SCHEMA_PROPERTY);

        return getPlatform().getAlterTablesSql(catalog, schema, null, desiredModel);
    }

    /**
     * Determines the value of the bean's property that has the given name. Depending on the
     * case-setting of the current builder, the case of teh name is considered or not. 
     * 
     * @param bean     The bean
     * @param propName The name of the property
     * @return The value
     */
    protected Object getPropertyValue(DynaBean bean, String propName)
    {
        if (getPlatform().isDelimitedIdentifierModeOn())
        {
            return bean.get(propName);
        }
        else
        {
            DynaProperty[] props = bean.getDynaClass().getDynaProperties();
    
            for (int idx = 0; idx < props.length; idx++)
            {
                if (propName.equalsIgnoreCase(props[idx].getName()))
                {
                    return bean.get(props[idx].getName());
                }
            }
            throw new IllegalArgumentException("The bean has no property with the name "+propName);
        }
    }
}
