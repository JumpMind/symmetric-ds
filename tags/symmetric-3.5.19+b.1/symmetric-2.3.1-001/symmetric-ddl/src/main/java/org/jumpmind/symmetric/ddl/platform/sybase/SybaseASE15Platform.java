package org.jumpmind.symmetric.ddl.platform.sybase;

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

/**
 * The platform implementation for Sybase ASE 15 and above.
 * 
 * @version $Revision:  $
 */
public class SybaseASE15Platform extends SybasePlatform
{
    /** Database name of this platform. */
    public static final String DATABASENAME     = "SybaseASE15";
    
    /**
     * Creates a new platform instance.
     */
    public SybaseASE15Platform()
    {
        super();
    }

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }
}
