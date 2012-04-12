package org.jumpmind.db.platform.hsqldb;

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

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.DatabasePlatformSettings;

/*
 * The platform implementation for the HsqlDb database.
 */
public class HsqlDbDatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard Hsqldb jdbc driver. */
    public static final String JDBC_DRIVER = "org.hsqldb.jdbcDriver";

    /* The subprotocol used by the standard Hsqldb driver. */
    public static final String JDBC_SUBPROTOCOL = "hsqldb";

    /*
     * Creates a new instance of the Hsqldb platform.
     */
    public HsqlDbDatabasePlatform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);

        info.setNonPKIdentityColumnsSupported(false);
        info.setIdentityOverrideAllowed(false);
        info.setSystemForeignKeyIndicesAlwaysNonUnique(true);

        info.addNativeTypeMapping(Types.ARRAY, "LONGVARBINARY", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BLOB, "LONGVARBINARY", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB, "LONGVARCHAR", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DISTINCT, "LONGVARBINARY", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "OBJECT");
        info.addNativeTypeMapping(Types.NULL, "LONGVARBINARY", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF, "LONGVARBINARY", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT, "LONGVARBINARY", Types.LONGVARBINARY);
        // JDBC's TINYINT requires a value range of -255 to 255, but HsqlDb's is
        // only -128 to 127
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);

        info.addNativeTypeMapping("BIT", "BOOLEAN", "BOOLEAN");
        info.addNativeTypeMapping("DATALINK", "LONGVARBINARY", "LONGVARBINARY");

        info.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        info.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        info.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        info.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);

        
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);

        ddlReader = new HsqlDbDdlReader(this);
        ddlBuilder = new HsqlDbDdlBuilder(this);

    }
    
    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new HsqlDbJdbcSqlTemplate(dataSource, settings, null);
    }

    public String getName() {
        return DatabaseNamesConstants.HSQLDB;
    }
    
    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return null;
    }
    
}
