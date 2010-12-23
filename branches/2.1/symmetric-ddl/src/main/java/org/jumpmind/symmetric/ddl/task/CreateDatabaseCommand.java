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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.tools.ant.BuildException;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Database;

/**
 * The sub task for creating the target database. Note that this is only supported on some database
 * platforms. See the database support documentation for details on which platforms support this.<br/>
 * This sub task does not require schema files. Therefore the <code>fileset</code> subelement and
 * the <code>schemaFile</code> attribute of the enclosing task can be omitted.
 * 
 * @version $Revision: 231306 $
 * @ant.task name="createDatabase"
 */
public class CreateDatabaseCommand extends DatabaseCommand
{
    /** The additional creation parameters. */
    private ArrayList _parameters = new ArrayList();

    /**
     * Adds a parameter which is a name-value pair.
     * 
     * @param param The parameter
     */
    public void addConfiguredParameter(Parameter param)
    {
        _parameters.add(param);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isRequiringModel()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DatabaseTaskBase task, Database model) throws BuildException
    {
        BasicDataSource dataSource = getDataSource();

        if (dataSource == null)
        {
            throw new BuildException("No database specified.");
        }

        Platform platform = getPlatform();
        
        try
        {
            platform.createDatabase(dataSource.getDriverClassName(),
                                    dataSource.getUrl(),
                                    dataSource.getUsername(),
                                    dataSource.getPassword(),
                                    getFilteredParameters(platform.getName()));

            _log.info("Created database");
        }
        catch (UnsupportedOperationException ex)
        {
            _log.error("Database platform " + platform.getName() + " does not support database creation " +
                       "via JDBC or there was an error while creating it.",
                       ex);
        }
        catch (Exception ex)
        {
            handleException(ex, ex.getMessage());
        }
    }

    /**
     * Filters the parameters for the indicated platform.
     * 
     * @param platformName The name of the platform
     * @return The filtered parameters
     */
    private Map getFilteredParameters(String platformName)
    {
        LinkedHashMap parameters = new LinkedHashMap();

        for (Iterator it = _parameters.iterator(); it.hasNext();)
        {
            Parameter param = (Parameter)it.next();

            if (param.isForPlatform(platformName))
            {
                parameters.put(param.getName(), param.getValue());
            }
        }
        return parameters;
    }
}
