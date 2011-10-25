package org.jumpmind.symmetric.ddl.platform.oracle;

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

/*
 * The platform for Oracle 8.
 * 
 * TODO: We might support the {@link org.jumpmind.symmetric.ddl.Platform#createDatabase(String, String, String, String, Map)}
 *       functionality via "CREATE SCHEMA"/"CREATE USER" or "CREATE TABLESPACE" ?
 *
 * @version $Revision: 231306 $
 */
public class Oracle8Platform extends PlatformImplBase
{
    /* Database name of this platform. */
    public static final String DATABASENAME              = "Oracle";
    /* The standard Oracle jdbc driver. */
    public static final String JDBC_DRIVER               = "oracle.jdbc.driver.OracleDriver";
    /* The old Oracle jdbc driver. */
    public static final String JDBC_DRIVER_OLD           = "oracle.jdbc.dnlddriver.OracleDriver";
    /* The thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_THIN     = "oracle:thin";
    /* The thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_OCI8     = "oracle:oci8";
    /* The old thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_THIN_OLD = "oracle:dnldthin";

    /*
     * Creates a new platform instance.
     */
    public Oracle8Platform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(30);
        info.setIdentityStatusReadingSupported(false);

        // Note that the back-mappings are partially done by the model reader, not the driver
        info.addNativeTypeMapping(Types.ARRAY,         "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.BIGINT,        "NUMBER(38)");
        info.addNativeTypeMapping(Types.BINARY,        "RAW",              Types.VARBINARY);
        info.addNativeTypeMapping(Types.BIT,           "NUMBER(1)");
        info.addNativeTypeMapping(Types.DATE,          "DATE",             Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.DECIMAL,       "NUMBER");
        info.addNativeTypeMapping(Types.DISTINCT,      "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.DOUBLE,        "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT,         "FLOAT",            Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT,   "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.LONGVARCHAR,   "CLOB",             Types.CLOB);
        info.addNativeTypeMapping(Types.NULL,          "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.NUMERIC,       "NUMBER",           Types.DECIMAL);
        info.addNativeTypeMapping(Types.INTEGER,       "NUMBER(22)",           Types.DECIMAL);
        info.addNativeTypeMapping(Types.OTHER,         "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.REF,           "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.SMALLINT,      "NUMBER(5)");
        info.addNativeTypeMapping(Types.STRUCT,        "BLOB",             Types.BLOB);
        info.addNativeTypeMapping(Types.TIME,          "DATE",             Types.DATE);
        info.addNativeTypeMapping(Types.TIMESTAMP,     "DATE");
        info.addNativeTypeMapping(Types.TINYINT,       "NUMBER(3)");
        info.addNativeTypeMapping(Types.VARBINARY,     "RAW");
        info.addNativeTypeMapping(Types.VARCHAR,       "VARCHAR2");

        info.addNativeTypeMapping("BOOLEAN",  "NUMBER(1)", "BIT");
        info.addNativeTypeMapping("DATALINK", "BLOB",      "BLOB");

        info.setDefaultSize(Types.CHAR,       254);
        info.setDefaultSize(Types.VARCHAR,    254);
        info.setDefaultSize(Types.BINARY,     254);
        info.setDefaultSize(Types.VARBINARY,  254);
        
        info.setStoresUpperCaseInCatalog(true);

        setSqlBuilder(new Oracle8Builder(this));
        setModelReader(new OracleModelReader(this));
    }

    /*
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }
}
