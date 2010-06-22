package org.jumpmind.symmetric.ddl.dynabean;

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

import org.jumpmind.symmetric.ddl.DdlUtilsException;

/**
 * This exception is thrown when something dealing with sql dyna beans or classes failed.
 * 
 * @version $Revision: 289996 $
 */
public class SqlDynaException extends DdlUtilsException
{
    /** Constant for serializing instances of this class. */
	private static final long serialVersionUID = 7904337501884384392L;

	/**
     * Creates a new empty exception object.
     */
    public SqlDynaException()
    {
        super();
    }

    /**
     * Creates a new exception object.
     * 
     * @param msg The exception message
     */
    public SqlDynaException(String msg)
    {
        super(msg);
    }

    /**
     * Creates a new exception object.
     * 
     * @param baseEx The base exception
     */
    public SqlDynaException(Throwable baseEx)
    {
        super(baseEx);
    }

    /**
     * Creates a new exception object.
     * 
     * @param msg    The exception message
     * @param baseEx The base exception
     */
    public SqlDynaException(String msg, Throwable baseEx)
    {
        super(msg, baseEx);
    }

}
