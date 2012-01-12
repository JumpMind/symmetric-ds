package org.jumpmind.db.platform.mssql;

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
import org.jumpmind.log.Log;

/*
 * The platform implementation for the Microsoft SQL Server database.
 */
public class MsSqlPlatform extends AbstractJdbcDatabasePlatform {

    /* Database name of this platform. */
    public static final String DATABASENAME = "MsSql";

    /* The standard SQLServer jdbc driver. */
    public static final String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

    /* The subprotocol used by the standard SQL Server driver. */
    public static final String JDBC_SUBPROTOCOL = "jtds";

    /*
     * Creates a new platform instance.
     */
    public MsSqlPlatform(DataSource dataSource, DatabasePlatformSettings settings, Log log) {
        super(dataSource, settings, log);

        info.setMaxIdentifierLength(128);

        info.addNativeTypeMapping(Types.ARRAY, "IMAGE", Types.LONGVARBINARY);
        // BIGINT will be mapped back to BIGINT by the model reader
        info.addNativeTypeMapping(Types.BIGINT, "DECIMAL(19,0)");
        info.addNativeTypeMapping(Types.BLOB, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DATE, "DATETIME", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.DISTINCT, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE, "FLOAT", Types.FLOAT);
        info.addNativeTypeMapping(Types.INTEGER, "INT");
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "IMAGE");
        info.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT");
        info.addNativeTypeMapping(Types.NULL, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT, "IMAGE", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TIME, "DATETIME", Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIMESTAMP, "DATETIME");
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping("BOOLEAN", "BIT", "BIT");
        info.addNativeTypeMapping("DATALINK", "IMAGE", "LONGVARBINARY");

        info.setDefaultSize(Types.CHAR, 254);
        info.setDefaultSize(Types.VARCHAR, 254);
        info.setDefaultSize(Types.BINARY, 254);
        info.setDefaultSize(Types.VARBINARY, 254);

        info.setStoresUpperCaseInCatalog(true);
        info.setDateOverridesToTimestamp(true);
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);
        info.setAutoIncrementUpdateAllowed(false);
        
        ddlReader = new MsSqlDdlReader(log, this);
        ddlBuilder = new MsSqlBuilder(log, this);

        setDelimitedIdentifierModeOn(true);
    }

    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new MsSqlJdbcSqlTemplate(dataSource, settings);
    }

    public String getName() {
        return DATABASENAME;
    }

    public String getDefaultCatalog() {
        if (StringUtils.isBlank(defaultCatalog)) {
            defaultCatalog = (String) getSqlTemplate().queryForObject("select DB_NAME()",
                    String.class);
        }
        return defaultCatalog;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select SCHEMA_NAME()",
                    String.class);
        }
        return defaultSchema;
    }

}
