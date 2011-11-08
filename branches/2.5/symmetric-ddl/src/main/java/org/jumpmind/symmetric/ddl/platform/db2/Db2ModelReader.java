package org.jumpmind.symmetric.ddl.platform.db2;

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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;

import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.jumpmind.symmetric.ddl.DdlUtilsException;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Index;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.platform.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.ddl.platform.JdbcModelReader;

/*
 * Reads a database model from a Db2 UDB database.
 *
 * @version $Revision: $
 */
public class Db2ModelReader extends JdbcModelReader
{
	/* Known system tables that Db2 creates (e.g. automatic maintenance). */
	private static final String[] KNOWN_SYSTEM_TABLES = { "STMG_DBSIZE_INFO", "HMON_ATM_INFO", "HMON_COLLECTION", "POLICY" };
	/* The regular expression pattern for the time values that Db2 returns. */
	private Pattern _db2TimePattern;
	/* The regular expression pattern for the timestamp values that Db2 returns. */
	private Pattern _db2TimestampPattern;

	/*
     * Creates a new model reader for Db2 databases.
     * 
     * @param platform The platform that this model reader belongs to
     */
    public Db2ModelReader(Platform platform)
    {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);

        PatternCompiler compiler = new Perl5Compiler();

    	try
    	{
    		_db2TimePattern      = compiler.compile("'(\\d{2}).(\\d{2}).(\\d{2})'");
    		_db2TimestampPattern = compiler.compile("'(\\d{4}\\-\\d{2}\\-\\d{2})\\-(\\d{2}).(\\d{2}).(\\d{2})(\\.\\d{1,8})?'");
        }
    	catch (MalformedPatternException ex)
        {
        	throw new DdlUtilsException(ex);
        }
    }

    @Override
	protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData, Map values) throws SQLException
	{
        String tableName = (String)values.get("TABLE_NAME");

        for (int idx = 0; idx < KNOWN_SYSTEM_TABLES.length; idx++)
        {
        	if (KNOWN_SYSTEM_TABLES[idx].equals(tableName))
        	{
        		return null;
        	}
        }

        Table table = super.readTable(connection, metaData, values);

        if (table != null)
        {
            // Db2 does not return the auto-increment status via the database metadata
            determineAutoIncrementFromResultSetMetaData(connection, table, table.getColumns());
        }
        return table;
	}

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException
    {
		Column column = super.readColumn(metaData, values);

		if (column.getDefaultValue() != null)
		{
			if (column.getTypeCode() == Types.TIME)
			{
				PatternMatcher matcher = new Perl5Matcher();

				// Db2 returns "HH24.MI.SS"
				if (matcher.matches(column.getDefaultValue(), _db2TimePattern))
				{
					StringBuffer newDefault = new StringBuffer();

					newDefault.append("'");
					// the hour
					newDefault.append(matcher.getMatch().group(1));
					newDefault.append(":");
					// the minute
					newDefault.append(matcher.getMatch().group(2));
					newDefault.append(":");
					// the second
					newDefault.append(matcher.getMatch().group(3));
					newDefault.append("'");

					column.setDefaultValue(newDefault.toString());
				}
			}
			else if (column.getTypeCode() == Types.TIMESTAMP)
			{
				PatternMatcher matcher = new Perl5Matcher();

				// Db2 returns "YYYY-MM-DD-HH24.MI.SS.FF"
				if (matcher.matches(column.getDefaultValue(), _db2TimestampPattern))
				{
					StringBuffer newDefault = new StringBuffer();

					newDefault.append("'");
					// group 1 is the date which has the correct format
					newDefault.append(matcher.getMatch().group(1));
					newDefault.append(" ");
					// the hour
					newDefault.append(matcher.getMatch().group(2));
					newDefault.append(":");
					// the minute
					newDefault.append(matcher.getMatch().group(3));
					newDefault.append(":");
					// the second
					newDefault.append(matcher.getMatch().group(4));
					// optionally, the fraction
					if ((matcher.getMatch().groups() > 4) && (matcher.getMatch().group(4) != null))
					{
						newDefault.append(matcher.getMatch().group(5));
					}
					newDefault.append("'");

					column.setDefaultValue(newDefault.toString());
				}
			}
            else if (TypeMap.isTextType(column.getTypeCode()))
            {
                column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
            }
		}
		return column;
	}

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection, DatabaseMetaDataWrapper metaData, Table table, Index index) throws SQLException
    {
        // Db2 uses the form "SQL060205225246220" if the primary key was defined during table creation
        // When the ALTER TABLE way was used however, the index has the name of the primary key
    	if (index.getName().startsWith("SQL"))
    	{
    		try
    		{
    			Long.parseLong(index.getName().substring(3));
    			return true;
    		}
    		catch (NumberFormatException ex)
    		{
    			// we ignore it
    		}
            return false;
    	}
        else
        {
            // we'll compare the index name to the names of all primary keys
            // TODO: Once primary key names are supported, this can be done easier via the table object
            ResultSet pkData  = null;
            HashSet   pkNames = new HashSet();

            try
            {
                pkData = metaData.getPrimaryKeys(table.getName());
                while (pkData.next())
                {
                    Map values = readColumns(pkData, getColumnsForPK());

                    pkNames.add(values.get("PK_NAME"));
                }
            }
            finally
            {
                if (pkData != null)
                {
                    pkData.close();
                }
            }

            return pkNames.contains(index.getName());
        }
    }
}
