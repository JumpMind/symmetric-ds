package org.jumpmind.symmetric.ddl.platform.maxdb;

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

import org.jumpmind.symmetric.ddl.platform.sapdb.SapDbPlatform;

/**
 * The platform implementation for MaxDB. It is currently identical to the SapDB
 * implementation as there is no difference in the functionality we're using.
 * Note that DdlUtils is currently not able to distinguish them based on the
 * jdbc driver or subprotocol as they are identical.   
 * 
 * @version $Revision: 231306 $
 */
public class MaxDbPlatform extends SapDbPlatform
{
    /** Database name of this platform. */
    public static final String DATABASENAME = "MaxDB";

    /**
     * Creates a new platform instance.
     */
    public MaxDbPlatform()
    {
        super();
        setSqlBuilder(new MaxDbBuilder(this));
        setModelReader(new MaxDbModelReader(this));
    }

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }
}
