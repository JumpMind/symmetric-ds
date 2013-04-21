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

import javax.sql.DataSource;

import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for the Interbase database.
 */
public class InterbaseDatabasePlatform extends AbstractJdbcDatabasePlatform {
    
    /* The interbase jdbc driver. */
    public static final String JDBC_DRIVER = "interbase.interclient.Driver";

    /* The subprotocol used by the interbase driver. */
    public static final String JDBC_SUBPROTOCOL = "interbase";

    /*
     * Creates a new platform instance.
     */
    public InterbaseDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    protected InterbaseDdlBuilder createDdlBuilder() {
        return new InterbaseDdlBuilder();
    }

    @Override
    protected InterbaseDdlReader createDdlReader() {
        return new InterbaseDdlReader(this);
    }    
    
    @Override
    protected InterbaseJdbcSqlTemplate createSqlTemplate() {
        return new InterbaseJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }

    public String getName() {
        return DatabaseNamesConstants.INTERBASE;
    }

    public String getDefaultCatalog() {     
        return null;
    }
    
    public String getDefaultSchema() {
        return null;
    }
    
}
