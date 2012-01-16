package org.jumpmind.db.platform.h2;

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

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.IDatabasePlatform;

/*
 * The platform implementation for the H2 database.
 */
public class H2Platform extends AbstractJdbcDatabasePlatform implements IDatabasePlatform {

    /* Database name of this platform. */
    public static final String[] DATABASENAMES = { "H2", "H21" };

    /* The standard H2 driver. */
    public static final String JDBC_DRIVER = "org.h2.Driver";

    /* The sub protocol used by the H2 driver. */
    public static final String JDBC_SUBPROTOCOL = "h2";

    /*
     * Creates a new instance of the H2 platform.
     */
    public H2Platform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);

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
        info.addNativeTypeMapping(Types.NUMERIC, "DECIMAL", Types.DECIMAL);
        info.addNativeTypeMapping(Types.BINARY, "BINARY", Types.BINARY);
        info.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        info.addNativeTypeMapping(Types.CLOB, "CLOB", Types.CLOB);
        info.addNativeTypeMapping(Types.LONGVARCHAR, "VARCHAR", Types.VARCHAR);
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        info.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        info.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        info.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        info.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);

        info.setStoresUpperCaseInCatalog(true);
        info.setNonBlankCharColumnSpacePadded(false);
        info.setBlankCharColumnSpacePadded(false);
        info.setCharColumnSpaceTrimmed(true);
        info.setEmptyStringNulled(false);

        ddlReader = new H2DdlReader(this);
        ddlBuilder = new H2Builder(this);
    }
    
    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new H2JdbcSqlTemplate(dataSource, settings, null);
    }

    public String getName() {
        return DATABASENAMES[0];
    }
    
    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select SCHEMA()", String.class);
        }
        return defaultSchema;
    }
    
    public String getDefaultCatalog() {
        return null;
    }

}