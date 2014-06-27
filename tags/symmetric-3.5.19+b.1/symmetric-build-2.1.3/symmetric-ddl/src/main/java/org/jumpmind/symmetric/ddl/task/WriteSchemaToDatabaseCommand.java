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

import org.apache.tools.ant.BuildException;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Database;
import org.jumpmind.symmetric.ddl.platform.CreationParameters;

/**
 * Parses the schema XML files specified for the enclosing task, and creates the corresponding
 * schema in the database.
 * 
 * @version $Revision: 289996 $
 * @ant.task name="writeSchemaToDatabase"
 */
public class WriteSchemaToDatabaseCommand extends DatabaseCommandWithCreationParameters
{
    /** Whether to alter or re-set the database if it already exists. */
    private boolean _alterDb = true;
    /** Whether to drop tables and the associated constraints if necessary. */
    private boolean _doDrops = true;

    /**
     * Determines whether to alter the database if it already exists, or re-set it.
     * 
     * @return <code>true</code> if to alter the database
     */
    protected boolean isAlterDatabase()
    {
        return _alterDb;
    }

    /**
     * Specifies whether DdlUtils shall alter an existing database rather than clearing it and
     * creating it new.
     * 
     * @param alterTheDb <code>true</code> if to alter the database
     * @ant.not-required Per default an existing database is altered
     */
    public void setAlterDatabase(boolean alterTheDb)
    {
        _alterDb = alterTheDb;
    }

    /**
     * Determines whether to drop tables and the associated constraints before re-creating them
     * (this implies <code>alterDatabase</code> is <code>false</code>).
     * 
     * @return <code>true</code> if drops shall be performed
     */
    protected boolean isDoDrops()
    {
        return _doDrops;
    }

    /**
     * Specifies whether tables, external constraints, etc. can be dropped if necessary.
     * Note that this is only relevant when <code>alterDatabase</code> is <code>false</code>.
     * 
     * @param doDrops <code>true</code> if drops shall be performed
     * @ant.not-required Per default database structures are dropped if necessary
     */
    public void setDoDrops(boolean doDrops)
    {
        _doDrops = doDrops;
    }

    /**
     * {@inheritDoc}
     */
    public void execute(DatabaseTaskBase task, Database model) throws BuildException
    {
        if (getDataSource() == null)
        {
            throw new BuildException("No database specified.");
        }

        Platform           platform        = getPlatform();
        boolean            isCaseSensitive = platform.isDelimitedIdentifierModeOn();
        CreationParameters params          = getFilteredParameters(model, platform.getName(), isCaseSensitive);

        platform.setScriptModeOn(false);
        // we're disabling the comment generation because we're writing directly to the database
        platform.setSqlCommentsOn(false);
        try
        {
            if (isAlterDatabase())
            {
                if ((getCatalogPattern() != null) || (getSchemaPattern() != null))
                {
                    platform.alterTables(getCatalogPattern(),
                                         getSchemaPattern(),
                                         null,
                                         model,
                                         params,
                                         true);
                }
                else
                {
                    platform.alterTables(model,
                                         params,
                                         true);
                }
            }
            else
            {
                platform.createTables(model,
                                      params,
                                      _doDrops,
                                      true);
            }

            _log.info("Written schema to database");
        }
        catch (Exception ex)
        {
            handleException(ex, ex.getMessage());
        }
    }
}
