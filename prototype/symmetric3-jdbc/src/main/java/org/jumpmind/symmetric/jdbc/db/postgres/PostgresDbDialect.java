package org.jumpmind.symmetric.jdbc.db.postgres;

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
import javax.sql.rowset.serial.SerialBlob;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.DmlStatement;
import org.jumpmind.symmetric.core.db.DmlStatement.DmlType;
import org.jumpmind.symmetric.core.db.postgres.PostgreTableBuilder;
import org.jumpmind.symmetric.core.db.postgres.PostgresDataCaptureBuilder;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.jdbc.db.AbstractJdbcDbDialect;
import org.jumpmind.symmetric.jdbc.db.JdbcSqlTemplate;

/**
 * The platform implementation for PostgresSql.
 */
public class PostgresDbDialect extends AbstractJdbcDbDialect {

    protected static final int[] DATA_INTEGRITY_SQL_ERROR_CODES = { 23000, 23502, 23503, 23505,
            23514 };

    /** Database name of this platform. */
    public static final String DATABASENAME = "PostgreSql";

    /** The standard PostgreSQL jdbc driver. */
    public static final String JDBC_DRIVER = "org.postgresql.Driver";

    /** The subprotocol used by the standard PostgreSQL driver. */
    public static final String JDBC_SUBPROTOCOL = "postgresql";

    /**
     * Creates a new platform instance.
     */
    public PostgresDbDialect(DataSource dataSource, Parameters parameters) {
        super(dataSource, parameters);

        // this is the default length though it might be changed when building
        // PostgreSQL
        // in file src/include/postgres_ext.h
        dialectInfo.setMaxIdentifierLength(31);

        dialectInfo.addNativeTypeMapping(Types.ARRAY, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.BINARY, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN");
        dialectInfo.addNativeTypeMapping(Types.BLOB, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.CLOB, "TEXT", Types.LONGVARCHAR);
        dialectInfo.addNativeTypeMapping(Types.DECIMAL, "NUMERIC", Types.NUMERIC);
        dialectInfo.addNativeTypeMapping(Types.DISTINCT, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.DOUBLE, "DOUBLE PRECISION");
        dialectInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE PRECISION", Types.DOUBLE);
        dialectInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.LONGVARBINARY, "BYTEA");
        dialectInfo.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.LONGVARCHAR);
        dialectInfo.addNativeTypeMapping(Types.NULL, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.OTHER, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.REF, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.STRUCT, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.SMALLINT);
        dialectInfo.addNativeTypeMapping(Types.VARBINARY, "BYTEA", Types.LONGVARBINARY);
        dialectInfo.addNativeTypeMapping("BOOLEAN", "BOOLEAN", "BIT");
        dialectInfo.addNativeTypeMapping("DATALINK", "BYTEA", "LONGVARBINARY");

        dialectInfo.setDefaultSize(Types.CHAR, 254);
        dialectInfo.setDefaultSize(Types.VARCHAR, 254);

        // no support for specifying the size for these types (because they are
        // mapped
        // to BYTEA which back-maps to BLOB)
        dialectInfo.setHasSize(Types.BINARY, false);
        dialectInfo.setHasSize(Types.VARBINARY, false);

        dialectInfo.setDelimitedIdentifierModeOn(true);

        // These come from the old db dialect class
        dialectInfo.setDateOverridesToTimestamp(false);
        dialectInfo.setEmptyStringNulled(false);
        dialectInfo.setBlankCharColumnSpacePadded(true);
        dialectInfo.setNonBlankCharColumnSpacePadded(true);
        dialectInfo.setRequiresAutoCommitFalseToSetFetchSize(true);

        this.dataCaptureBuilder = new PostgresDataCaptureBuilder(this);
        this.jdbcModelReader = new PostgresJdbcTableReader(this);
        this.tableBuilder = new PostgreTableBuilder(this);
    }

    @Override
    protected void validateParameters(Parameters parameters) {
        int beforeFlush = parameters.getInt(Parameters.LOADER_MAX_ROWS_BEFORE_BATCH_FLUSH, 1000);
        int beforeCommit = parameters.getInt(Parameters.LOADER_MAX_ROWS_BEFORE_COMMIT, 1000);
        if (beforeFlush != beforeCommit) {
            log.warn("%s must be equal to %s for postgres.  Setting %s to %s.",
                    Parameters.LOADER_MAX_ROWS_BEFORE_BATCH_FLUSH,
                    Parameters.LOADER_MAX_ROWS_BEFORE_COMMIT,
                    Parameters.LOADER_MAX_ROWS_BEFORE_BATCH_FLUSH,
                    Integer.toString(beforeCommit));
            parameters.put(Parameters.LOADER_MAX_ROWS_BEFORE_BATCH_FLUSH,
                    Integer.toString(beforeCommit));
        }
    }

    @Override
    public JdbcSqlTemplate getJdbcSqlConnection() {
        return new PostgresJdbcSqlTemplate(this);
    }

    @Override
    protected int[] getDataIntegritySqlErrorCodes() {
        return DATA_INTEGRITY_SQL_ERROR_CODES;
    }

    @Override
    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData) {

        Object[] objectValues = super.getObjectValues(encoding, values, orderedMetaData);
        for (int i = 0; i < orderedMetaData.length; i++) {
            if (orderedMetaData[i] != null && orderedMetaData[i].getTypeCode() == Types.BLOB
                    && objectValues[i] != null) {
                try {
                    objectValues[i] = new SerialBlob((byte[]) objectValues[i]);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return objectValues;
    }
    
    @Override
    protected String cleanTextForTextBasedColumns(String text) {
        return text.replace("\0", "");
    }

    @Override
    public String getDefaultSchema() {
        String defaultSchema = super.getDefaultSchema();
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select current_schema()",
                    String.class);
        }
        return defaultSchema;
    }

    @Override
    public boolean requiresSavepoints() {
        return true;
    }

    @Override
    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, Column[] preFilteredColumns) {
        return new PostgresDmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                preFilteredColumns, dialectInfo.isDateOverridesToTimestamp(),
                dialectInfo.getIdentifierQuoteString());
    }
}
