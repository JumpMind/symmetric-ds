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

import org.apache.log4j.Level;
import org.apache.tools.ant.types.EnumeratedAttribute;

/**
 * Helper class that defines the possible values for the verbosity attribute.
 * 
 * @ant.task ignore="true"
 */
public class VerbosityLevel extends EnumeratedAttribute {
    /** The possible levels. */
    private static final String[] LEVELS = { Level.FATAL.toString().toUpperCase(),
                                             Level.ERROR.toString().toUpperCase(),
                                             Level.WARN.toString().toUpperCase(),
                                             Level.INFO.toString().toUpperCase(),
                                             Level.DEBUG.toString().toUpperCase(),
                                             Level.FATAL.toString().toLowerCase(),
                                             Level.ERROR.toString().toLowerCase(),
                                             Level.WARN.toString().toLowerCase(),
                                             Level.INFO.toString().toLowerCase(),
                                             Level.DEBUG.toString().toLowerCase() };

    /**
     * Creates an uninitialized verbosity level object.
     */
    public VerbosityLevel()
    {
        super();
    }

    /**
     * Creates an initialized verbosity level object.
     * 
     * @param level The level
     */
    public VerbosityLevel(String level)
    {
        super();
        setValue(level);
    }

    /**
     * {@inheritDoc}
     */
    public String[] getValues() {
        return LEVELS;
    }

    /**
     * Determines whether this is DEBUG verbosity.
     * 
     * @return <code>true</code> if this is the DEBUG level
     */
    public boolean isDebug()
    {
        return Level.DEBUG.toString().equalsIgnoreCase(getValue());
    }
}