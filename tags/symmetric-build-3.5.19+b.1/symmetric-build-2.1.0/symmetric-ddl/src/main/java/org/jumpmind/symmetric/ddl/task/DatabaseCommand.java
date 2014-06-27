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

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.tools.ant.BuildException;
import org.jumpmind.symmetric.ddl.Platform;

/**
 * Base type for commands that have the database info embedded.
 * 
 * @version $Revision: 289996 $
 * @ant.type ignore="true"
 */
public abstract class DatabaseCommand extends Command
{
    /** The platform configuration. */
    private PlatformConfiguration _platformConf = new PlatformConfiguration();

    /**
     * Returns the database type.
     * 
     * @return The database type
     */
    protected String getDatabaseType()
    {
        return _platformConf.getDatabaseType();
    }

    /**
     * Returns the data source to use for accessing the database.
     * 
     * @return The data source
     */
    protected BasicDataSource getDataSource()
    {
        return _platformConf.getDataSource();
    }

    /**
     * Returns the catalog pattern if any.
     * 
     * @return The catalog pattern
     */
    public String getCatalogPattern()
    {
        return _platformConf.getCatalogPattern();
    }

    /**
     * Returns the schema pattern if any.
     * 
     * @return The schema pattern
     */
    public String getSchemaPattern()
    {
        return _platformConf.getSchemaPattern();
    }

    /**
     * Sets the platform configuration.
     * 
     * @param platformConf The platform configuration
     */
    protected void setPlatformConfiguration(PlatformConfiguration platformConf)
    {
        _platformConf = platformConf;
    }

    /**
     * Creates the platform for the configured database.
     * 
     * @return The platform
     */
    protected Platform getPlatform() throws BuildException
    {
        return _platformConf.getPlatform();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isRequiringModel()
    {
        return true;
    }
}
