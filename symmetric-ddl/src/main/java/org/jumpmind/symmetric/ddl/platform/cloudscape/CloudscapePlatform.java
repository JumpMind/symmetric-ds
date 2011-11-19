package org.jumpmind.symmetric.ddl.platform.cloudscape;

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

import java.sql.Types;

import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * The Cloudscape platform implementation.
 * 
 * @version $Revision: 231306 $
 */
public class CloudscapePlatform extends PlatformImplBase
{
    /** Database name of this platform. */
    public static final String DATABASENAME = "Cloudscape";
    /** A subprotocol used by the DB2 network driver. */
    public static final String JDBC_SUBPROTOCOL_1 = "db2j:net";
    /** A subprotocol used by the DB2 network driver. */
    public static final String JDBC_SUBPROTOCOL_2 = "cloudscape:net";

    /**
     * Creates a new platform instance.
     */
    public CloudscapePlatform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(128);
        info.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        // BINARY and VARBINARY will also be handled by CloudscapeBuilder.getSqlType
        info.addNativeTypeMapping(Types.ARRAY,         "BLOB",                     Types.BLOB);
        info.addNativeTypeMapping(Types.BINARY,        "CHAR {0} FOR BIT DATA");
        info.addNativeTypeMapping(Types.BIT,           "SMALLINT",                 Types.SMALLINT);
        info.addNativeTypeMapping(Types.DISTINCT,      "BLOB",                     Types.BLOB);
        info.addNativeTypeMapping(Types.DOUBLE,        "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT,         "DOUBLE PRECISION",         Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT,   "BLOB",                     Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "LONG VARCHAR FOR BIT DATA");
        info.addNativeTypeMapping(Types.LONGVARCHAR,   "LONG VARCHAR");
        info.addNativeTypeMapping(Types.NULL,          "LONG VARCHAR FOR BIT DATA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER,         "BLOB",                      Types.BLOB);
        info.addNativeTypeMapping(Types.REF,           "LONG VARCHAR FOR BIT DATA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT,        "BLOB",                      Types.BLOB);
        info.addNativeTypeMapping(Types.TINYINT,       "SMALLINT",                  Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY,     "VARCHAR {0} FOR BIT DATA");
        info.addNativeTypeMapping("BOOLEAN",  "SMALLINT",                  "SMALLINT");
        info.addNativeTypeMapping("DATALINK", "LONG VARCHAR FOR BIT DATA", "LONGVARBINARY");

        info.setDefaultSize(Types.BINARY,    254);
        info.setDefaultSize(Types.CHAR,      254);
        info.setDefaultSize(Types.VARBINARY, 254);
        info.setDefaultSize(Types.VARCHAR,   254);

        setSqlBuilder(new CloudscapeBuilder(this));
    }

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }
}
