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

import java.sql.Types;

import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * The DB2 platform implementation.
 * 
 * @version $Revision: 231306 $
 */
public class Db2Platform extends PlatformImplBase
{
    /** Database name of this platform. */
    public static final String DATABASENAME       = "DB2";
    /** The standard DB2 jdbc driver. */
    public static final String JDBC_DRIVER        = "com.ibm.db2.jcc.DB2Driver";
    /** Older name for the jdbc driver. */
    public static final String JDBC_DRIVER_OLD1   = "COM.ibm.db2.jdbc.app.DB2Driver";
    /** Older name for the jdbc driver. */
    public static final String JDBC_DRIVER_OLD2   = "COM.ibm.db2os390.sqlj.jdbc.DB2SQLJDriver";
    /** The JTOpen JDBC Driver. */
    public static final String JDBC_DRIVER_JTOPEN = "com.ibm.as400.access.AS400JDBCDriver";
    /** The subprotocol used by the standard DB2 driver. */
    public static final String JDBC_SUBPROTOCOL = "db2";
    /** An alternative subprotocol used by the standard DB2 driver on OS/390. */
    public static final String JDBC_SUBPROTOCOL_OS390_1 = "db2os390";
    /** An alternative subprotocol used by the standard DB2 driver on OS/390. */
    public static final String JDBC_SUBPROTOCOL_OS390_2 = "db2os390sqlj";
    /** An alternative subprotocol used by the JTOpen driver on OS/400. */
    public static final String JDBC_SUBPROTOCOL_JTOPEN = "as400";

    /**
     * Creates a new platform instance.
     */
    public Db2Platform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(18);

        // the BINARY types are also handled by Db2Builder.getSqlType(Column)
        info.addNativeTypeMapping(Types.ARRAY,         "BLOB",                      Types.BLOB);
        info.addNativeTypeMapping(Types.BINARY,        "CHAR {0} FOR BIT DATA");
        info.addNativeTypeMapping(Types.BIT,           "SMALLINT",                  Types.SMALLINT);
        info.addNativeTypeMapping(Types.FLOAT,         "DOUBLE",                    Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT,   "BLOB",                      Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "LONG VARCHAR FOR BIT DATA");
        info.addNativeTypeMapping(Types.LONGVARCHAR,   "LONG VARCHAR");
        info.addNativeTypeMapping(Types.NULL,          "LONG VARCHAR FOR BIT DATA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.NUMERIC,       "DECIMAL",                   Types.DECIMAL);
        info.addNativeTypeMapping(Types.OTHER,         "BLOB",                      Types.BLOB);
        info.addNativeTypeMapping(Types.STRUCT,        "BLOB",                      Types.BLOB);
        info.addNativeTypeMapping(Types.TINYINT,       "SMALLINT",                  Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY,     "VARCHAR {0} FOR BIT DATA");
        info.addNativeTypeMapping("BOOLEAN", "SMALLINT", "SMALLINT");

        info.setDefaultSize(Types.CHAR,      254);
        info.setDefaultSize(Types.VARCHAR,   254);
        info.setDefaultSize(Types.BINARY,    254);
        info.setDefaultSize(Types.VARBINARY, 254);

        setSqlBuilder(new Db2Builder(this));
        setModelReader(new Db2ModelReader(this));
    }

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }
}
