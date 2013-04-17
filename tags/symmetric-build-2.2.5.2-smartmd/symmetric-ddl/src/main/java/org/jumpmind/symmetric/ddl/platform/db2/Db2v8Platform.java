package org.jumpmind.symmetric.ddl.platform.db2;

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
 * The DB2 platform implementation for DB2 v8 and above.
 * 
 * @version $Revision: $
 */
public class Db2v8Platform extends Db2Platform
{
    /** Database name of this platform. */
    public static final String[] DATABASENAMES = {"DB2v8","DB2/LINUX9","DB2"};

    /**
     * Creates a new platform instance.
     */
    public Db2v8Platform()
    {
        super();
        // Note that we optimistically assume that number of characters = number of bytes
        // If the name contains characters that are more than one byte in the database's
        // encoding, then the db will report an error anyway, but we cannot really calculate
        // the number of bytes
        getPlatformInfo().setMaxIdentifierLength(128);
        getPlatformInfo().setMaxColumnNameLength(128);
        getPlatformInfo().setMaxConstraintNameLength(128);
        getPlatformInfo().setMaxForeignKeyNameLength(128);
        setSqlBuilder(new Db2v8Builder(this));
    }
}
