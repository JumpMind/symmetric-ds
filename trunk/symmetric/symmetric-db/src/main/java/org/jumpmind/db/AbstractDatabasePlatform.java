package org.jumpmind.db;

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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;

/*
 * Base class for platform implementations.
 */
public abstract class AbstractDatabasePlatform implements IDatabasePlatform {

    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";

    /* The default name for models read from the database, if no name as given. */
    protected static final String MODEL_DEFAULT_NAME = "default";

    /* The log for this platform. */
    protected Log log = LogFactory.getLog(getClass());

    /* The platform info. */
    protected DatabasePlatformInfo info = new DatabasePlatformInfo();

    /* The model reader for this platform. */
    protected IDdlReader ddlReader;

    protected IDdlBuilder ddlBuilder;

    protected Database cachedModel = new Database();

    private long lastTimeCachedModelClearedInMs = System.currentTimeMillis();

    protected long clearCacheModelTimeoutInMs = DateUtils.MILLIS_PER_HOUR;

    /* Whether script mode is on. */
    protected boolean scriptModeOn;

    /* Whether SQL comments are generated or not. */
    protected boolean sqlCommentsOn = false;

    /* Whether delimited identifiers are used or not. */
    protected boolean delimitedIdentifierModeOn;

    /* Whether identity override is enabled. */
    protected boolean identityOverrideOn;

    /* Whether read foreign keys shall be sorted alphabetically. */
    protected boolean foreignKeysSorted;

    protected String defaultSchema;

    protected String defaultCatalog;

    protected int[] primaryKeyViolationCodes;

    protected String[] primaryKeyViolationSqlStates;

    public AbstractDatabasePlatform(Log log) {
        this.log = log;
    }

    abstract public ISqlTemplate getSqlTemplate();

    public DmlStatement createDmlStatement(DmlType dmlType, Table table) {
        return createDmlStatement(dmlType, table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), table.getColumns());
    }

    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns) {
        return new DmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                getPlatformInfo().isDateOverridesToTimestamp(), getPlatformInfo()
                        .getIdentifierQuoteString());
    }

    public void setLog(Log log) {
        this.log = log;
    }

    public IDdlReader getDdlReader() {
        return ddlReader;
    }

    public IDdlBuilder getDdlBuilder() {
        return ddlBuilder;
    }

    public DatabasePlatformInfo getPlatformInfo() {
        return info;
    }

    public void setClearCacheModelTimeoutInMs(long clearCacheModelTimeoutInMs) {
        this.clearCacheModelTimeoutInMs = clearCacheModelTimeoutInMs;
    }

    public long getClearCacheModelTimeoutInMs() {
        return clearCacheModelTimeoutInMs;
    }

    public boolean isScriptModeOn() {
        return scriptModeOn;
    }

    public void setScriptModeOn(boolean scriptModeOn) {
        this.scriptModeOn = scriptModeOn;
    }

    public boolean isSqlCommentsOn() {
        return sqlCommentsOn;
    }

    public void setSqlCommentsOn(boolean sqlCommentsOn) {
        if (!getPlatformInfo().isSqlCommentsSupported() && sqlCommentsOn) {
            throw new DdlException("Platform does not support SQL comments");
        }
        this.sqlCommentsOn = sqlCommentsOn;
    }

    public boolean isDelimitedIdentifierModeOn() {
        return delimitedIdentifierModeOn;
    }

    public void setDelimitedIdentifierModeOn(boolean delimitedIdentifierModeOn) {
        if (!getPlatformInfo().isDelimitedIdentifiersSupported() && delimitedIdentifierModeOn) {
            throw new DdlException("Platform does not support delimited identifier");
        }
        this.delimitedIdentifierModeOn = delimitedIdentifierModeOn;
    }

    public boolean isIdentityOverrideOn() {
        return identityOverrideOn;
    }

    public void setIdentityOverrideOn(boolean identityOverrideOn) {
        this.identityOverrideOn = identityOverrideOn;
    }

    public boolean isForeignKeysSorted() {
        return foreignKeysSorted;
    }

    public void setForeignKeysSorted(boolean foreignKeysSorted) {
        this.foreignKeysSorted = foreignKeysSorted;
    }

    public void dropDatabase(Database database, boolean continueOnError) {
        String sql = ddlBuilder.dropTables(database);
        new SqlScript(sql, getSqlTemplate(), !continueOnError).execute(true);
    }

    public void createDatabase(Database targetDatabase, boolean dropTablesFirst,
            boolean continueOnError) {
        if (dropTablesFirst) {
            dropDatabase(targetDatabase, true);
        }
        String createSql = ddlBuilder.createTables(targetDatabase, false);

        if (log.isDebugEnabled()) {
            log.debug("Generated create sql: \n", createSql);
        }

        String delimiter = info.getSqlCommandDelimiter();
        new SqlScript(createSql, getSqlTemplate(), !continueOnError, delimiter, null).execute();
    }

    public void alterDatabase(Database desiredDatabase, boolean continueOnError) {
        alterTables(continueOnError, desiredDatabase.getTables());
    }

    public void alterTables(boolean continueOnError, Table... desiredTables) {
        Database currentDatabase = new Database();
        Database desiredDatabase = new Database();
        for (Table table : desiredTables) {
            desiredDatabase.addTable(table);
            Table currentTable = ddlReader.readTable(table.getCatalog(), table.getSchema(),
                    table.getName());
            if (currentTable != null) {
                currentDatabase.addTable(currentTable);
            }
        }

        String alterSql = ddlBuilder.alterDatabase(currentDatabase, desiredDatabase);

        if (log.isDebugEnabled()) {
            log.debug("Generated alter sql: \n", alterSql);
        }
        String delimiter = info.getSqlCommandDelimiter();
        new SqlScript(alterSql, getSqlTemplate(), !continueOnError, delimiter, null).execute();

    }

    public Database readDatabase(String catalog, String schema, String[] tableTypes) {
        Database model = ddlReader.readTables(catalog, schema, tableTypes);
        if ((model.getName() == null) || (model.getName().length() == 0)) {
            model.setName(MODEL_DEFAULT_NAME);
        }
        return model;
    }

    public Table readTableFromDatabase(String catalogName, String schemaName, String tablename) {
        return ddlReader.readTable(catalogName, schemaName, tablename);
    }

    public void resetCachedTableModel() {
        synchronized (this.getClass()) {
            this.cachedModel.resetTableIndexCache();
            Table[] tables = this.cachedModel.getTables();
            if (tables != null) {
                for (Table table : tables) {
                    this.cachedModel.removeTable(table);
                }
            }
        }
    }

    public Table getTableFromCache(String tableName, boolean forceReread) {
        return getTableFromCache(null, null, tableName, forceReread);
    }

    public Table getTableFromCache(String catalogName, String schemaName, String tableName,
            boolean forceReread) {
        if (System.currentTimeMillis() - lastTimeCachedModelClearedInMs > clearCacheModelTimeoutInMs) {
            synchronized (this.getClass()) {
                cachedModel = new Database();
                lastTimeCachedModelClearedInMs = System.currentTimeMillis();
            }
        }
        catalogName = catalogName == null ? getDefaultCatalog() : catalogName;
        schemaName = schemaName == null ? getDefaultSchema() : schemaName;
        Database model = cachedModel;
        Table retTable = model != null ? model.findTable(catalogName, schemaName, tableName) : null;
        if (retTable == null || forceReread) {
            synchronized (this.getClass()) {
                try {
                    Table table = readTableFromDatabase(catalogName, schemaName, tableName);

                    if (retTable != null) {
                        cachedModel.removeTable(retTable);
                    }

                    if (table != null) {
                        cachedModel.addTable(table);
                    }

                    retTable = table;
                } catch (RuntimeException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
        return retTable;
    }

    public Object[] getObjectValues(BinaryEncoding encoding, Table table, String[] columnNames,
            String[] values) {
        Column[] metaData = Table.orderColumns(columnNames, table);
        return getObjectValues(encoding, values, metaData);
    }

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData) {
        List<Object> list = new ArrayList<Object>(values.length);
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            Object objectValue = value;
            Column column = orderedMetaData.length > i ? orderedMetaData[i] : null;
            try {
                if (column != null) {
                    int type = column.getTypeCode();
                    if ((value == null || (info.isEmptyStringNulled() && value.equals("")))
                            && column.isRequired() && column.isOfTextType()) {
                        objectValue = REQUIRED_FIELD_NULL_SUBSTITUTE;
                    }
                    if (value != null) {
                        if (type == Types.DATE && !info.isDateOverridesToTimestamp()) {
                            objectValue = parseDate(value);
                        } else if (type == Types.TIMESTAMP
                                || (type == Types.DATE && info.isDateOverridesToTimestamp())) {
                            objectValue = Timestamp.valueOf(value);
                        } else if (type == Types.CHAR) {
                            String charValue = value.toString();
                            if ((StringUtils.isBlank(charValue) && info
                                    .isBlankCharColumnSpacePadded())
                                    || (StringUtils.isNotBlank(charValue) && info
                                            .isNonBlankCharColumnSpacePadded())) {
                                objectValue = StringUtils.rightPad(value.toString(),
                                        column.getSizeAsInt(), ' ');
                            }
                        } else if (type == Types.INTEGER || type == Types.SMALLINT
                                || type == Types.BIT) {
                            objectValue = Integer.valueOf(value);
                        } else if (type == Types.NUMERIC || type == Types.DECIMAL
                                || type == Types.FLOAT || type == Types.DOUBLE) {
                            // The number will have either one period or one
                            // comma
                            // for the decimal point, but we need a period
                            objectValue = new BigDecimal(value.replace(',', '.'));
                        } else if (type == Types.BOOLEAN) {
                            objectValue = value.equals("1") ? Boolean.TRUE : Boolean.FALSE;
                        } else if (type == Types.BLOB || type == Types.LONGVARBINARY
                                || type == Types.BINARY || type == Types.VARBINARY ||
                                // SQLServer ntext type
                                type == -10) {
                            if (encoding == BinaryEncoding.NONE) {
                                objectValue = value.getBytes();
                            } else if (encoding == BinaryEncoding.BASE64) {
                                objectValue = Base64.decodeBase64(value.getBytes());
                            } else if (encoding == BinaryEncoding.HEX) {
                                objectValue = Hex.decodeHex(value.toCharArray());
                            }
                        } else if (type == Types.TIME) {
                            objectValue = new Time(Timestamp.valueOf(value).getTime());
                        } else if (type == Types.ARRAY) {
                            objectValue = createArray(column, value);
                        }
                    }
                    if (objectValue instanceof String) {
                        objectValue = cleanTextForTextBasedColumns((String) objectValue);
                    }
                    list.add(objectValue);
                }
            } catch (Exception ex) {
                log.error("Could not convert a value of %s for column %s of type %s", value,
                        column.getName(), column.getType(), ex);
                throw new RuntimeException(ex);
            }
        }

        return list.toArray();
    }

    protected Array createArray(Column column, final String value) {
        return null;
    }

    protected String cleanTextForTextBasedColumns(String text) {
        return text;
    }

    protected java.util.Date parseDate(String value) {
        try {
            return DateUtils.parseDate(value, TIMESTAMP_PATTERNS);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isClob(int type) {
        return type == Types.CLOB || type == Types.LONGVARCHAR;
    }

    public boolean isBlob(int type) {
        return type == Types.BLOB || type == Types.BINARY || type == Types.VARBINARY
                || type == Types.LONGVARBINARY || type == -10;
    }
    
    public List<Column> getLobColumns(Table table) {
        List<Column> lobColumns = new ArrayList<Column>(1);
        Column[] allColumns = table.getColumns();
        for (Column column : allColumns) {
            if (isLob(column.getTypeCode())) {
                lobColumns.add(column);
            }
        }
        return lobColumns;
    }

    public boolean isLob(int type) {
        return type == Types.CLOB || type == Types.BLOB || type == Types.BINARY
                || type == Types.VARBINARY || type == Types.LONGVARBINARY
                || type == Types.LONGNVARCHAR ||
                // SQL-Server ntext binary type
                type == -10;
    }

}
