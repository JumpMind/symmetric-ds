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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.DatabaseMetaDataWrapper;

/**
 * Reads a database model from an Oracle 10 database.
 *
 * @version $Revision: $
 */
public class Oracle10ModelReader extends Oracle8ModelReader
{
    /**
     * Creates a new model reader for Oracle 10 databases.
     * 
     * @param platform The platform that this model reader belongs to
     */
    public Oracle10ModelReader(Platform platform)
    {
        super(platform);
    }

    /**
     * {@inheritDoc}
     */
	protected Table readTable(DatabaseMetaDataWrapper metaData, Map values) throws SQLException
	{
		// Oracle 10 added the recycle bin which contains dropped database objects not yet purged
		// Since we don't want entries from the recycle bin, we filter them out
        PreparedStatement stmt       = null;
        boolean           deletedObj = false;

        try
        {
        	stmt = getConnection().prepareStatement("SELECT * FROM RECYCLEBIN WHERE OBJECT_NAME=?");
        	stmt.setString(1, (String)values.get("TABLE_NAME"));
        	
        	ResultSet rs = stmt.executeQuery();

        	if (rs.next())
        	{
        		// we found the table in the recycle bin, so its a deleted one which we ignore
        		deletedObj = true;
        	}
        	rs.close();
        }
        finally
        {
        	if (stmt != null)
        	{
        		stmt.close();
        	}
        }

        return deletedObj ? null : super.readTable(metaData, values);
	}

}
