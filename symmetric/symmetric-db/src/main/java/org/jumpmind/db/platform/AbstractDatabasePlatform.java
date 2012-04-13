package org.jumpmind.db.platform;

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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Base class for platform implementations.
 */
public abstract class AbstractDatabasePlatform implements IDatabasePlatform {

    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

    public static final String[] TIME_PATTERNS = { "HH:mm:ss.S", "HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss" };

    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";

    /* The default name for models read from the database, if no name as given. */
    protected static final String MODEL_DEFAULT_NAME = "default";

    /* The log for this platform. */
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /* The platform info. */
    protected DatabasePlatformInfo info = new DatabasePlatformInfo();

    /* The model reader for this platform. */
    protected IDdlReader ddlReader;

    protected IDdlBuilder ddlBuilder;

    protected Map<String, Table> tableCache = new HashMap<String, Table>();

    private long lastTimeCachedModelClearedInMs = System.currentTimeMillis();

    protected long clearCacheModelTimeoutInMs = DateUtils.MILLIS_PER_HOUR;

    /* Whether identity override is enabled. */
    protected boolean identityOverrideOn;

    /* Whether read foreign keys shall be sorted alphabetically. */
    protected boolean foreignKeysSorted;

    protected String defaultSchema;

    protected String defaultCatalog;

    protected Boolean storesUpperCaseIdentifiers;

    protected Boolean storesLowerCaseIdentifiers;

    protected Boolean storesMixedCaseIdentifiers;

    protected boolean metadataIgnoreCase = true;

    public AbstractDatabasePlatform() {
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
                        .getDelimiterToken());
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

    public Table readTableFromDatabase(String catalogName, String schemaName, String tableName) {
        String originalFullyQualifiedName = Table.getFullyQualifiedTableName(catalogName,
                schemaName, tableName);
        Table table = ddlReader.readTable(catalogName, schemaName, tableName);
        if (table == null && metadataIgnoreCase) {
            if (isStoresUpperCaseIdentifiers()) {
                catalogName = StringUtils.upperCase(catalogName);
                schemaName = StringUtils.upperCase(schemaName);
                tableName = StringUtils.upperCase(tableName);
                // if we didn't find the table, the database stores upper case,
                // and the catalog, schema or table were not in upper case
                // already then it is probably stored in uppercase
                if (!originalFullyQualifiedName.equals(Table.getFullyQualifiedTableName(
                        catalogName, schemaName, tableName))) {
                    table = ddlReader.readTable(catalogName, schemaName, tableName);
                }
            } else if (isStoresLowerCaseIdentifiers()) {
                catalogName = StringUtils.lowerCase(catalogName);
                schemaName = StringUtils.lowerCase(schemaName);
                tableName = StringUtils.lowerCase(tableName);
                // if we didn't find the table, the database stores lower case,
                // and the catalog, schema or table were not in lower case
                // already then it is probably stored in uppercase
                if (!originalFullyQualifiedName.equals(Table.getFullyQualifiedTableName(
                        catalogName, schemaName, tableName))) {
                    table = ddlReader.readTable(catalogName, schemaName, tableName);
                }
            } else {
                tableName = StringUtils.lowerCase(tableName);
                // Last ditch lower case effort. This case applied to
                // symmetricds tables stored in a mixed case schema or catalog
                // whose default case is different on the source system than on
                // the target system.
                if (!originalFullyQualifiedName.equals(Table.getFullyQualifiedTableName(
                        catalogName, schemaName, tableName))) {
                    table = ddlReader.readTable(catalogName, schemaName, tableName);
                }
            }

            if (table == null) {
                // Last ditch upper case effort. This case applied to
                // symmetricds tables stored in a mixed case schema or catalog
                // whose default case is different on the source system than on
                // the target system.
                tableName = StringUtils.upperCase(tableName);
                if (!originalFullyQualifiedName.equals(Table.getFullyQualifiedTableName(
                        catalogName, schemaName, tableName))) {
                    table = ddlReader.readTable(catalogName, schemaName, tableName);
                }
            }
        }
        return table;
    }

    public void resetCachedTableModel() {
        synchronized (this.getClass()) {
            this.tableCache = new HashMap<String, Table>();
            lastTimeCachedModelClearedInMs = System.currentTimeMillis();
        }
    }

    public Table getTableFromCache(String tableName, boolean forceReread) {
        return getTableFromCache(null, null, tableName, forceReread);
    }

    public Table getTableFromCache(String catalogName, String schemaName, String tableName,
            boolean forceReread) {
        if (System.currentTimeMillis() - lastTimeCachedModelClearedInMs > clearCacheModelTimeoutInMs) {
            resetCachedTableModel();
        }
        catalogName = catalogName == null ? getDefaultCatalog() : catalogName;
        schemaName = schemaName == null ? getDefaultSchema() : schemaName;
        Map<String, Table> model = tableCache;
        String key = Table.getFullyQualifiedTableName(catalogName, schemaName, tableName);
        Table retTable = model != null ? model.get(key) : null;
        if (retTable == null || forceReread) {
            synchronized (this.getClass()) {
                try {
                    Table table = readTableFromDatabase(catalogName, schemaName, tableName);
                    tableCache.put(key, table);
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
                            objectValue = parseTime(value);
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
                log.error("Could not convert a value of {} for column {} of type {}", new Object[] {
                        value, column.getName(), column.getType() });
                log.error(ex.getMessage(), ex);
                throw new RuntimeException(ex);
            }
        }

        return list.toArray();
    }

    public String[] getStringValues(Column[] metaData, Row row) {
        String[] values = new String[metaData.length];
        for (int i = 0; i < metaData.length; i++) {
            if (metaData[i].isOfTextType()) {
                values[i] = row.getString(metaData[i].getName());
            } else if (metaData[i].isOfNumericType()) {
                values[i] = row.getString(metaData[i].getName());
            } else if (metaData[i].isOfBinaryType()) {
                values[i] = row.getString(metaData[i].getName());
            }
        }
        return values;
    }

    public Map<String, String> getSqlScriptReplacementTokens() {
        return null;
    }

    public String scrubSql(String sql) {
        Map<String, String> replacementTokens = getSqlScriptReplacementTokens();
        if (replacementTokens != null) {
            return FormatUtils.replaceTokens(sql, replacementTokens, false).trim();
        } else {
            return sql;
        }
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

    protected java.util.Date parseTime(String value) {
        try {
            return DateUtils.parseDate(value, TIME_PATTERNS);
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

    public void setMetadataIgnoreCase(boolean metadataIgnoreCase) {
        this.metadataIgnoreCase = metadataIgnoreCase;
    }

    public boolean isMetadataIgnoreCase() {
        return metadataIgnoreCase;
    }

    public boolean isStoresLowerCaseIdentifiers() {
        if (storesLowerCaseIdentifiers == null) {
            storesLowerCaseIdentifiers = getSqlTemplate().isStoresLowerCaseIdentifiers();
        }
        return storesLowerCaseIdentifiers;
    }

    public boolean isStoresMixedCaseQuotedIdentifiers() {
        if (storesMixedCaseIdentifiers == null) {
            storesMixedCaseIdentifiers = getSqlTemplate().isStoresMixedCaseQuotedIdentifiers();
        }
        return storesMixedCaseIdentifiers;
    }

    public boolean isStoresUpperCaseIdentifiers() {
        if (storesUpperCaseIdentifiers == null) {
            storesUpperCaseIdentifiers = getSqlTemplate().isStoresUpperCaseIdentifiers();
        }
        return storesUpperCaseIdentifiers;
    }

    public boolean isLob(int type) {
        return type == Types.CLOB || type == Types.BLOB || type == Types.BINARY
                || type == Types.VARBINARY || type == Types.LONGVARBINARY
                || type == Types.LONGNVARCHAR ||
                // SQL-Server ntext binary type
                type == -10;
    }

    public Database readDatabaseFromXml(String filePath, boolean alterCaseToMatchDatabaseDefaultCase) {
        InputStream is = null;
        File file = new File(filePath);
        if (file.exists()) {
            try {
                is = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw new IoException(e);
            }
        } else {
            is = AbstractDatabasePlatform.class.getResourceAsStream(filePath);
        }

        if (is != null) {
            InputStreamReader reader = new InputStreamReader(is);
            Database database = new DatabaseIO().read(reader);
            IOUtils.closeQuietly(reader);
            if (alterCaseToMatchDatabaseDefaultCase) {
                boolean storesUpperCase = isStoresUpperCaseIdentifiers();
                Table[] tables = database.getTables();
                for (Table table : tables) {
                    if (!FormatUtils.isMixedCase(table.getName())) {
                        table.setName(storesUpperCase ? table.getName().toUpperCase() : table
                                .getName().toLowerCase());
                    }

                    Column[] columns = table.getColumns();
                    for (Column column : columns) {
                        if (!FormatUtils.isMixedCase(column.getName())) {
                            column.setName(storesUpperCase ? column.getName().toUpperCase()
                                    : column.getName().toLowerCase());
                        }
                    }

                    IIndex[] indexes = table.getIndices();
                    for (IIndex index : indexes) {
                        if (!FormatUtils.isMixedCase(index.getName())) {
                            index.setName(storesUpperCase ? index.getName().toUpperCase() : index
                                    .getName().toLowerCase());
                        }

                        IndexColumn[] indexColumns = index.getColumns();
                        for (IndexColumn indexColumn : indexColumns) {
                            if (!FormatUtils.isMixedCase(indexColumn.getName())) {
                                indexColumn.setName(storesUpperCase ? indexColumn.getName()
                                        .toUpperCase() : indexColumn.getName().toLowerCase());
                            }
                        }
                    }
                }
            }
            return database;
        } else {
            throw new IoException("Could not find the file: %s", filePath);
        }

    }

}
