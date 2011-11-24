package org.jumpmind.db.platform.postgresql;

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

import org.jumpmind.db.AbstractDatabasePlatform;
import org.jumpmind.db.platform.AbstractDdlBuilder;

/*
 * The platform implementation for PostgresSql.
 */
public class PostgreSqlPlatform extends AbstractDatabasePlatform {
    /* Database name of this platform. */
    public static final String DATABASENAME = "PostgreSql";
    /* The standard PostgreSQL jdbc driver. */
    public static final String JDBC_DRIVER = "org.postgresql.Driver";
    /* The subprotocol used by the standard PostgreSQL driver. */
    public static final String JDBC_SUBPROTOCOL = "postgresql";

    /*
     * Creates a new platform instance.
     */
    public PostgreSqlPlatform() {

        // this is the default length though it might be changed when building
        // PostgreSQL
        // in file src/include/postgres_ext.h
        info.setMaxIdentifierLength(31);

        info.addNativeTypeMapping(Types.ARRAY, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BINARY, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BIT, "BOOLEAN");
        info.addNativeTypeMapping(Types.BLOB, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DECIMAL, "NUMERIC", Types.NUMERIC);
        info.addNativeTypeMapping(Types.DISTINCT, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BYTEA");
        info.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.NULL, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY, "BYTEA", Types.LONGVARBINARY);
        info.addNativeTypeMapping("BOOLEAN", "BOOLEAN", "BIT");
        info.addNativeTypeMapping("DATALINK", "BYTEA", "LONGVARBINARY");

        info.setDefaultSize(Types.CHAR, 254);
        info.setDefaultSize(Types.VARCHAR, 254);

        // no support for specifying the size for these types (because they are
        // mapped
        // to BYTEA which back-maps to BLOB)
        info.setHasSize(Types.BINARY, false);
        info.setHasSize(Types.VARBINARY, false);

        setDelimitedIdentifierModeOn(true);

        ddlReader = new PostgreSqlDdlReader(log, this);
        ddlBuilder = new PostgreSqlBuilder(log, this);
    }

    public String getName() {
        return DATABASENAME;
    }

}
