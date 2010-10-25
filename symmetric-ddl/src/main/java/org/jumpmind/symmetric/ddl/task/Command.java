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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tools.ant.BuildException;
import org.jumpmind.symmetric.ddl.model.Database;

/**
 * Base class for commands that work with a model.
 * 
 * @version $Revision: 289996 $
 * @ant.type ignore="true"
 */
public abstract class Command
{
    /** The log. */
    protected final Log _log = LogFactory.getLog(getClass());

    /** Whether to stop execution upon an error. */
    private boolean _failOnError = true;

    /**
     * Determines whether the command execution will be stopped upon an error.
     * Default value is <code>true</code>.
     *
     * @return <code>true</code> if the execution stops in case of an error
     */
    public boolean isFailOnError()
    {
        return _failOnError;
    }

    /**
     * Specifies whether the execution shall stop if an error has occurred during the task runs.
     *
     * @param failOnError <code>true</code> if the execution shall stop in case of an error
     * @ant.not-required By default execution will be stopped when an error is encountered.
     */
    public void setFailOnError(boolean failOnError)
    {
        _failOnError = failOnError;
    }

    /**
     * Handles the given exception according to the fail-on-error setting by either
     * re-throwing it (wrapped in a build exception) or only logging it.
     * 
     * @param ex  The exception
     * @param msg The message to use unless this the exception is rethrown and it is
     *            already a build exception 
     */
    protected void handleException(Exception ex, String msg) throws BuildException
    {
        if (isFailOnError())
        {
            if (ex instanceof BuildException)
            {
                throw (BuildException)ex;
            }
            else
            {
                throw new BuildException(msg, ex);
            }
        }
        else
        {
            _log.error(msg, ex);
        }
    }
    
    /**
     * Specifies whether this command requires a model, i.e. whether the second
     * argument in {@link #execute(DatabaseTaskBase, Database)} cannot be <code>null</code>.
     * 
     * @return <code>true</code> if this command requires a model 
     */
    public abstract boolean isRequiringModel();

    /**
     * Executes this command.
     * 
     * @param task  The executing task
     * @param model The database model
     */
    public abstract void execute(DatabaseTaskBase task, Database model) throws BuildException;
}
