package org.jumpmind.db.platform.sqlite;

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

import javax.sql.DataSource;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.util.Log;

/*
 * The platform implementation for the SQLite database.
 */
public class SqLitePlatform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    /* Database name of this platform. */
    public static final String DATABASENAME = "SQLite3";

    /* The standard H2 driver. */
    public static final String JDBC_DRIVER = "org.sqlite.JDBC";

    /*
     * Creates a new instance of the H2 platform.
     */
    public SqLitePlatform(DataSource dataSource, Log log) {
        super(dataSource, log);

        info.setNonPKIdentityColumnsSupported(false);
        info.setIdentityOverrideAllowed(false);
        info.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        info.setNullAsDefaultValueRequired(false);
        info.addNativeTypeMapping(Types.ARRAY, "BINARY", Types.BINARY);
        info.addNativeTypeMapping(Types.DISTINCT, "BINARY", Types.BINARY);
        info.addNativeTypeMapping(Types.NULL, "BINARY", Types.BINARY);
        info.addNativeTypeMapping(Types.REF, "BINARY", Types.BINARY);
        info.addNativeTypeMapping(Types.STRUCT, "BINARY", Types.BINARY);
        info.addNativeTypeMapping(Types.DATALINK, "BINARY", Types.BINARY);

        info.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.TINYINT);
        info.addNativeTypeMapping(Types.SMALLINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.BINARY, "BINARY", Types.BINARY);
        info.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.CLOB, "CLOB", Types.CLOB);
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        info.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        info.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        info.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        info.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);

        ddlReader = new SqLiteDdlReader(log, this);
        ddlBuilder = new SqLiteBuilder(log, this); 
    }

    public String getName() {
        return DATABASENAME;
    }

}