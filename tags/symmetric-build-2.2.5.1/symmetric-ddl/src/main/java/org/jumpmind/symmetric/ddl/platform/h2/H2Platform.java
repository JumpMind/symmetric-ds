package org.jumpmind.symmetric.ddl.platform.h2;

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

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * The platform implementation for the H2 database. From patch at <a
 * href="https://issues.apache.org/jira/browse/DDLUTILS-185"
 * >https://issues.apache.org/jira/browse/DDLUTILS-185</a>
 * 
 * @version $Revision: 231306 $
 */
public class H2Platform extends PlatformImplBase implements Platform {

    /** Database name of this platform. */
    public static final String[] DATABASENAMES = {"H2","H21"};
    
    /** The standard H2 driver. */
    public static final String JDBC_DRIVER = "org.h2.Driver";
    
    /** The sub protocol used by the H2 driver. */
    public static final String JDBC_SUBPROTOCOL = "h2";

    /**
     * Creates a new instance of the H2 platform.
     */
    public H2Platform() {
        PlatformInfo info = getPlatformInfo();

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

        info.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);;
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

        setSqlBuilder(new H2Builder(this));
        setModelReader(new H2ModelReader(this));
    }

    /**
     * {@inheritDoc}
     */
    public String getName() {
        return DATABASENAMES[0];
    }

}