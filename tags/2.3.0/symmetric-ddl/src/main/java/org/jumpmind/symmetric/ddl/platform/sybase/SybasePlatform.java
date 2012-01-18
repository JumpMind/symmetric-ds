package org.jumpmind.symmetric.ddl.platform.sybase;

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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.jumpmind.symmetric.ddl.DatabaseOperationException;
import org.jumpmind.symmetric.ddl.PlatformInfo;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.model.TypeMap;
import org.jumpmind.symmetric.ddl.platform.PlatformImplBase;

/**
 * The platform implementation for Sybase.
 * 
 * @version $Revision: 231306 $
 */
public class SybasePlatform extends PlatformImplBase
{
    /** Database name of this platform. */
    public static final String DATABASENAME     = "Sybase";
    /** The standard Sybase jdbc driver. */
    public static final String JDBC_DRIVER      = "com.sybase.jdbc2.jdbc.SybDriver";
    /** The old Sybase jdbc driver. */
    public static final String JDBC_DRIVER_OLD  = "com.sybase.jdbc.SybDriver";
    /** The subprotocol used by the standard Sybase driver. */
    public static final String JDBC_SUBPROTOCOL = "sybase:Tds";

    /** The maximum size that text and binary columns can have. */
    public static final long MAX_TEXT_SIZE = 2147483647;
    
    /**
     * Creates a new platform instance.
     */
    public SybasePlatform()
    {
        PlatformInfo info = getPlatformInfo();

        info.setMaxIdentifierLength(128);
        info.setNullAsDefaultValueRequired(true);
        info.setCommentPrefix("/*");
        info.setCommentSuffix("*/");
        info.setDelimiterToken("\"");
        setDelimitedIdentifierModeOn(true);

        info.addNativeTypeMapping(Types.ARRAY,         "IMAGE");
        // BIGINT is mapped back in the model reader
        info.addNativeTypeMapping(Types.BIGINT,        "DECIMAL(19,0)");
        // we're not using the native BIT type because it is rather limited (cannot be NULL, cannot be indexed)
        info.addNativeTypeMapping(Types.BIT,           "SMALLINT",         Types.SMALLINT);
        info.addNativeTypeMapping(Types.BLOB,          "IMAGE",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.CLOB,          "TEXT",             Types.LONGVARCHAR);
        info.addNativeTypeMapping(Types.DATE,          "DATETIME",         Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.DISTINCT,      "IMAGE",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.DOUBLE,        "DOUBLE PRECISION");
        info.addNativeTypeMapping(Types.FLOAT,         "DOUBLE PRECISION", Types.DOUBLE);
        info.addNativeTypeMapping(Types.INTEGER,       "INT");
        info.addNativeTypeMapping(Types.JAVA_OBJECT,   "IMAGE",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.LONGVARBINARY, "IMAGE");
        info.addNativeTypeMapping(Types.LONGVARCHAR,   "TEXT");
        info.addNativeTypeMapping(Types.NULL,          "IMAGE",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.OTHER,         "IMAGE",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.REF,           "IMAGE",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.STRUCT,        "IMAGE",            Types.LONGVARBINARY);
        info.addNativeTypeMapping(Types.TIME,          "DATETIME",         Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TIMESTAMP,     "DATETIME",         Types.TIMESTAMP);
        info.addNativeTypeMapping(Types.TINYINT,       "SMALLINT",         Types.SMALLINT);
        info.addNativeTypeMapping("BOOLEAN",  "SMALLINT", "SMALLINT");
        info.addNativeTypeMapping("DATALINK", "IMAGE",    "LONGVARBINARY");

        info.setDefaultSize(Types.BINARY,    254);
        info.setDefaultSize(Types.VARBINARY, 254);
        info.setDefaultSize(Types.CHAR,      254);
        info.setDefaultSize(Types.VARCHAR,   254);

        setSqlBuilder(new SybaseBuilder(this));
        setModelReader(new SybaseModelReader(this));
    }

    /**
     * {@inheritDoc}
     */
    public String getName()
    {
        return DATABASENAME;
    }

    /**
     * Sets the text size which is the maximum amount of bytes that Sybase returns in a SELECT statement
     * for binary/text columns (e.g. blob, longvarchar etc.).
     * 
     * @param size The size to set
     */
    private void setTextSize(long size)
    {
    	Connection connection = borrowConnection();
    	Statement  stmt       = null;

    	try
    	{
    		stmt = connection.createStatement();

    		stmt.execute("SET textsize "+size);
    	}
    	catch (SQLException ex)
    	{
    		throw new DatabaseOperationException(ex);
    	}
    	finally
    	{
    		closeStatement(stmt);
    		returnConnection(connection);
    	}
    }

    /**
     * {@inheritDoc}
     */
	protected Object extractColumnValue(ResultSet resultSet, String columnName, int columnIdx, int jdbcType) throws DatabaseOperationException, SQLException
	{
        boolean useIdx = (columnName == null);

        if ((jdbcType == Types.LONGVARBINARY) || (jdbcType == Types.BLOB))
		{
			InputStream stream = useIdx ? resultSet.getBinaryStream(columnIdx) : resultSet.getBinaryStream(columnName);

			if (stream == null)
			{
				return null;
			}
			else
			{
				byte[] buf    = new byte[65536];
				byte[] result = new byte[0];
				int    len;
	
				try
				{
					do
					{
						len = stream.read(buf);
						if (len > 0)
						{
							byte[] newResult = new byte[result.length + len];
	
							System.arraycopy(result, 0, newResult, 0, result.length);
							System.arraycopy(buf, 0, newResult, result.length, len);
							result = newResult;
						}
					}
					while (len > 0);
					stream.close();
					return result;
				}
				catch (IOException ex)
				{
					throw new DatabaseOperationException("Error while extracting the value of column " + columnName + " of type " +
							                             TypeMap.getJdbcTypeName(jdbcType) + " from a result set", ex);
				}
			}
		}
		else
		{
			return super.extractColumnValue(resultSet, columnName, columnIdx, jdbcType);
		}
	}

	/**
     * {@inheritDoc}
     */
	protected void setStatementParameterValue(PreparedStatement statement, int sqlIndex, int typeCode, Object value) throws SQLException
	{
        if ((typeCode == Types.BLOB) || (typeCode == Types.LONGVARBINARY))
        {
            // jConnect doesn't like the BLOB type, but works without problems with LONGVARBINARY
            // even when using the Blob class
            if (value instanceof byte[])
            {
                byte[] data = (byte[])value;

                statement.setBinaryStream(sqlIndex, new ByteArrayInputStream(data), data.length);
            }
            else
            {
                // Sybase doesn't like the BLOB type, but works without problems with LONGVARBINARY
                // even when using the Blob class
                super.setStatementParameterValue(statement, sqlIndex, Types.LONGVARBINARY, value);
            }
        }
		else if (typeCode == Types.CLOB)
		{
			// Same for CLOB and LONGVARCHAR
			super.setStatementParameterValue(statement, sqlIndex, Types.LONGVARCHAR, value);
		}
		else
		{
			super.setStatementParameterValue(statement, sqlIndex, typeCode, value);
		}
	}

    /**
     * Determines whether we need to use identity override mode for the given table.
     * 
     * @param table The table
     * @return <code>true</code> if identity override mode is needed
     */
    private boolean useIdentityOverrideFor(Table table)
    {
        return isIdentityOverrideOn() &&
               getPlatformInfo().isIdentityOverrideAllowed() &&
               (table.getAutoIncrementColumns().length > 0);
    }

    /**
     * {@inheritDoc}
     */
    protected void beforeInsert(Connection connection, Table table) throws SQLException
    {
        if (useIdentityOverrideFor(table))
        {
            SybaseBuilder builder          = (SybaseBuilder)getSqlBuilder();
            String        quotationOn      = builder.getQuotationOnStatement();
            String        identityInsertOn = builder.getEnableIdentityOverrideSql(table);
            Statement     stmt             = connection.createStatement();

            if (quotationOn.length() > 0)
            {
                stmt.execute(quotationOn);
            }
            stmt.execute(identityInsertOn);
            stmt.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void afterInsert(Connection connection, Table table) throws SQLException
    {
        if (useIdentityOverrideFor(table))
        {
            SybaseBuilder builder           = (SybaseBuilder)getSqlBuilder();
            String        quotationOn       = builder.getQuotationOnStatement();
            String        identityInsertOff = builder.getDisableIdentityOverrideSql(table);
            Statement     stmt              = connection.createStatement();

            if (quotationOn.length() > 0)
            {
                stmt.execute(quotationOn);
            }
            stmt.execute(identityInsertOff);
            stmt.close();
        }
    }

    /**
     * {@inheritDoc}
     */
    protected void beforeUpdate(Connection connection, Table table) throws SQLException
    {
        beforeInsert(connection, table);
    }

    /**
     * {@inheritDoc}
     */
    protected void afterUpdate(Connection connection, Table table) throws SQLException
    {
        afterInsert(connection, table);
    }
}
