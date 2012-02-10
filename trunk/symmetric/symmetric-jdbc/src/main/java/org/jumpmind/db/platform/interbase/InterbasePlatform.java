package org.jumpmind.db.platform.interbase;

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
 * The platform implementation for the Interbase database.
 */
public class InterbasePlatform extends AbstractJdbcDatabasePlatform {
    
    /* Database name of this platform. */
    public static final String DATABASENAME = DatabaseNamesConstants.INTERBASE;

    /* The interbase jdbc driver. */
    public static final String JDBC_DRIVER = "interbase.interclient.Driver";

    /* The subprotocol used by the interbase driver. */
    public static final String JDBC_SUBPROTOCOL = "interbase";

    public static int SWITCH_TO_LONGVARCHAR_SIZE = 4096;

    /*
     * Creates a new platform instance.
     */
    public InterbasePlatform(DataSource dataSource, DatabasePlatformSettings settings) {
        super(dataSource, settings);

        info.setMaxIdentifierLength(31);
        info.setCommentPrefix("/*");
        info.setCommentSuffix("*/");
        info.setSystemForeignKeyIndicesAlwaysNonUnique(true);

        // BINARY and VARBINARY are also handled by the
        // InterbaseBuilder.getSqlType method
        info.addNativeTypeMapping(Types.ARRAY, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BIGINT, "NUMERIC(18,0)");
        // Theoretically we could use (VAR)CHAR CHARACTER SET OCTETS but the
        // JDBC driver is not
        // able to handle that properly (the byte[]/BinaryStream accessors do
        // not work)
        info.addNativeTypeMapping(Types.BINARY, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.BIT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.BLOB, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB, "BLOB SUB_TYPE TEXT");
        info.addNativeTypeMapping(Types.DISTINCT, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        info.addNativeTypeMapping(Types.JAVA_OBJECT, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARCHAR, "VARCHAR(" + SWITCH_TO_LONGVARCHAR_SIZE + ")",
                Types.VARCHAR);
        info.addNativeTypeMapping(Types.NULL, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REAL, "FLOAT");
        info.addNativeTypeMapping(Types.REF, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        info.addNativeTypeMapping(Types.VARBINARY, "BLOB", Types.LONGVARBINARY);
        info.addNativeTypeMapping("BOOLEAN", "SMALLINT", "SMALLINT");
        info.addNativeTypeMapping("DATALINK", "BLOB", "LONGVARBINARY");

        info.setDefaultSize(Types.CHAR, 254);
        info.setDefaultSize(Types.VARCHAR, 254);
        info.setHasSize(Types.BINARY, false);
        info.setHasSize(Types.VARBINARY, false);

        info.setStoresUpperCaseInCatalog(true);
        info.setNonBlankCharColumnSpacePadded(true);
        info.setBlankCharColumnSpacePadded(true);
        info.setCharColumnSpaceTrimmed(false);
        info.setEmptyStringNulled(false);        

        ddlReader = new InterbaseDdlReader(this);
        ddlBuilder = new InterbaseBuilder(this);
    }
    
    @Override
    protected void createSqlTemplate() {
        this.sqlTemplate = new InterbaseJdbcSqlTemplate(dataSource, settings, null);
    }

    public String getName() {
        return DATABASENAME;
    }

    public String getDefaultCatalog() {     
        return null;
    }
    
    public String getDefaultSchema() {
        return null;
    }
    
}
