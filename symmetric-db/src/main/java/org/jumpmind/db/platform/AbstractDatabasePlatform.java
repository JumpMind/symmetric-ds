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
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
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

    /* The log for this platform. */
    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";

    /* The default name for models read from the database, if no name as given. */
    protected static final String MODEL_DEFAULT_NAME = "default";

    /* The model reader for this platform. */
    protected IDdlReader ddlReader;

    protected IDdlBuilder ddlBuilder;

    protected Map<String, Table> tableCache = new HashMap<String, Table>();

    private long lastTimeCachedModelClearedInMs = System.currentTimeMillis();

    protected long clearCacheModelTimeoutInMs = DateUtils.MILLIS_PER_HOUR;

    protected String defaultSchema;

    protected String defaultCatalog;

    protected Boolean storesUpperCaseIdentifiers;

    protected Boolean storesLowerCaseIdentifiers;

    protected Boolean storesMixedCaseIdentifiers;

    protected boolean metadataIgnoreCase = true;

    public AbstractDatabasePlatform() {
    }

    public DatabaseInfo getDatabaseInfo() {
        return getDdlBuilder().getDatabaseInfo();
    }

    abstract public ISqlTemplate getSqlTemplate();

    public DmlStatement createDmlStatement(DmlType dmlType, Table table) {
        return createDmlStatement(dmlType, table.getCatalog(), table.getSchema(), table.getName(),
                table.getPrimaryKeyColumns(), table.getColumns(), null);
    }

    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, boolean[] nullKeyValues) {
        return new DmlStatement(dmlType, catalogName, schemaName, tableName, keys, columns,
                getDdlBuilder().getDatabaseInfo().isDateOverridesToTimestamp(), getDdlBuilder()
                        .getDatabaseInfo().getDelimiterToken(), nullKeyValues);
    }

    public IDdlReader getDdlReader() {
        return ddlReader;
    }

    public IDdlBuilder getDdlBuilder() {
        return ddlBuilder;
    }

    public void setClearCacheModelTimeoutInMs(long clearCacheModelTimeoutInMs) {
        this.clearCacheModelTimeoutInMs = clearCacheModelTimeoutInMs;
    }

    public long getClearCacheModelTimeoutInMs() {
        return clearCacheModelTimeoutInMs;
    }

    public void dropDatabase(Database database, boolean continueOnError) {
        String sql = ddlBuilder.dropTables(database);
        new SqlScript(sql, getSqlTemplate(), !continueOnError).execute(true);
    }

    public void createTables(boolean dropTablesFirst, boolean continueOnError, Table... tables) {
        Database database = new Database();
        database.addTables(tables);
        createDatabase(database, dropTablesFirst, continueOnError);
    }
    
    public void createDatabase(Database targetDatabase, boolean dropTablesFirst,
            boolean continueOnError) {
        if (dropTablesFirst) {
            dropDatabase(targetDatabase, true);
        }
        
        String createSql = ddlBuilder.createTables(targetDatabase, false);

        if (log.isDebugEnabled()) {
            log.debug("Generated create sql: \n{}", createSql);
        }

        String delimiter = getDdlBuilder().getDatabaseInfo().getSqlCommandDelimiter();
        new SqlScript(createSql, getSqlTemplate(), !continueOnError, delimiter, null).execute();
    }

    public void alterDatabase(Database desiredDatabase, boolean continueOnError) {
        alterTables(continueOnError, desiredDatabase.getTables());
    }

    public void alterTables(boolean continueOnError, Table... desiredTables) {
        Database currentDatabase = new Database();
        Database desiredDatabase = new Database();
        StringBuilder tablesProcessed = new StringBuilder();
        for (Table table : desiredTables) {
            tablesProcessed.append(table.getFullyQualifiedTableName());
            tablesProcessed.append(", ");
            desiredDatabase.addTable(table);
            Table currentTable = ddlReader.readTable(table.getCatalog(), table.getSchema(),
                    table.getName());
            if (currentTable != null) {
                currentDatabase.addTable(currentTable);
            }
        }

        if (tablesProcessed.length() > 1) {
            tablesProcessed.replace(tablesProcessed.length() - 2, tablesProcessed.length(), "");
        }

        String alterSql = ddlBuilder.alterDatabase(currentDatabase, desiredDatabase);

        if (StringUtils.isNotBlank(alterSql.trim())) {
            log.info("Running alter sql:\n{}", alterSql);
            String delimiter = getDdlBuilder().getDatabaseInfo().getSqlCommandDelimiter();
            new SqlScript(alterSql, getSqlTemplate(), !continueOnError, delimiter, null).execute();
        } else {
            log.info("Tables up to date.  No alters found for {}", tablesProcessed);
        }

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

        if (table != null && log.isDebugEnabled()) {
            log.debug("Just read table: \n{}", table.toVerboseString());
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

    public Object[] getObjectValues(BinaryEncoding encoding, Table table, String[] columnNames,
            String[] values, boolean useVariableDates) {
        Column[] metaData = Table.orderColumns(columnNames, table);
        return getObjectValues(encoding, values, metaData, useVariableDates);
    }

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData) {
        return getObjectValues(encoding, values, orderedMetaData, false);
    }

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData, boolean useVariableDates) {
        if (values != null) {
            List<Object> list = new ArrayList<Object>(values.length);
            for (int i = 0; i < values.length; i++) {
                String value = values[i];
                Object objectValue = value;
                Column column = orderedMetaData.length > i ? orderedMetaData[i] : null;
                try {
                    if (column != null) {
                        int type = column.getMappedTypeCode();
                        if ((value == null || (getDdlBuilder().getDatabaseInfo()
                                .isEmptyStringNulled() && value.equals("")))
                                && column.isRequired()
                                && column.isOfTextType()) {
                            objectValue = REQUIRED_FIELD_NULL_SUBSTITUTE;
                        }
                        if (value != null) {
                            if (type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
                                objectValue = parseDate(type, value, useVariableDates);
                            } else if (type == Types.CHAR) {
                                String charValue = value.toString();
                                if ((StringUtils.isBlank(charValue) && getDdlBuilder()
                                        .getDatabaseInfo().isBlankCharColumnSpacePadded())
                                        || (StringUtils.isNotBlank(charValue) && getDdlBuilder()
                                                .getDatabaseInfo()
                                                .isNonBlankCharColumnSpacePadded())) {
                                    objectValue = StringUtils.rightPad(value.toString(),
                                            column.getSizeAsInt(), ' ');
                                }
                            } else if (type == Types.INTEGER || type == Types.SMALLINT
                                    || type == Types.BIT) {
                                objectValue = parseIntegerObjectValue(value);
                            } else if (type == Types.NUMERIC || type == Types.DECIMAL
                                    || type == Types.FLOAT || type == Types.DOUBLE
                                    || type == Types.REAL) {
                                // The number will have either one period or one
                                // comma for the decimal point, but we need a
                                // period
                                objectValue = new BigDecimal(value.replace(',', '.'));
                            } else if (type == Types.BOOLEAN) {
                                objectValue = value.equals("1") ? Boolean.TRUE : Boolean.FALSE;
                            } else if (!(column.getJdbcTypeName()!=null && column.getJdbcTypeName().toUpperCase()
                                    .contains(TypeMap.GEOMETRY)) && 
                                    (type == Types.BLOB || type == Types.LONGVARBINARY
                                    || type == Types.BINARY || type == Types.VARBINARY ||
                                    // SQLServer ntext type
                                    type == -10)) {
                                if (encoding == BinaryEncoding.NONE) {
                                    objectValue = value.getBytes();
                                } else if (encoding == BinaryEncoding.BASE64) {
                                    objectValue = Base64.decodeBase64(value.getBytes());
                                } else if (encoding == BinaryEncoding.HEX) {
                                    objectValue = Hex.decodeHex(value.toCharArray());
                                }
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
                    String valueTrimmed = FormatUtils.abbreviateForLogging(value);
                    log.error("Could not convert a value of {} for column {} of type {}",
                            new Object[] { valueTrimmed, column.getName(), column.getMappedType() });
                    log.error(ex.getMessage(), ex);
                    throw new RuntimeException(ex);
                }
            }

            return list.toArray();
        } else {
            return null;
        }
    }
    
    protected Number parseIntegerObjectValue(String value) {
        return new BigInteger(value);
    }

    // TODO: this should be AbstractDdlBuilder.getInsertSql(Table table,
    // Map<String, Object> columnValues, boolean genPlaceholders)
    public String[] getStringValues(BinaryEncoding encoding, Column[] metaData, Row row,
            boolean useVariableDates) {
        String[] values = new String[metaData.length];
        for (int i = 0; i < metaData.length; i++) {
            Column column = metaData[i];
            String name = column.getName();
            int type = column.getJdbcTypeCode();
            
            if (row.get(name) != null) {
                if (column.isOfNumericType()) {
                    values[i] = row.getString(name);
                } else if (!column.isTimestampWithTimezone() && 
                        (type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME)) {
                    Date date = row.getDateTime(name);
                    if (useVariableDates) {
                        long diff = date.getTime() - System.currentTimeMillis();
                        values[i] = "${curdate" + diff + "}";
                    } else if (type == Types.TIME) {
                        values[i] = FormatUtils.TIME_FORMATTER.format(date);
                    } else {
                        values[i] = FormatUtils.TIMESTAMP_FORMATTER.format(date);
                    }
                } else if (column.isOfBinaryType()) {
                    byte[] bytes = row.getBytes(name);
                    if (encoding == BinaryEncoding.NONE) {
                        values[i] = row.getString(name);
                    } else if (encoding == BinaryEncoding.BASE64) {
                        values[i] = new String(Base64.encodeBase64(bytes));
                    } else if (encoding == BinaryEncoding.HEX) {
                        values[i] = new String(Hex.encodeHex(bytes));
                    }
                } else {
                    values[i] = row.getString(name);
                }
            }
        }
        return values;
    }

    public String replaceSql(String sql, BinaryEncoding encoding, Table table, Row row,
            boolean useVariableDates) {
        final String QUESTION_MARK = "<!QUESTION_MARK!>";
        String newSql = sql;
        String quote = getDatabaseInfo().getValueQuoteToken();
        String regex = "\\?";
        
        List<Column> columnsToProcess = new ArrayList<Column>();
        columnsToProcess.addAll(table.getColumnsAsList());
        columnsToProcess.addAll(table.getPrimaryKeyColumnsAsList());
        
        for (int i = 0; i < columnsToProcess.size(); i++) {
            Column column = columnsToProcess.get(i);
            String name = column.getName();
            int type = column.getJdbcTypeCode();

            if (row.get(name) != null) {
                if (column.isOfTextType()) {
                    try {
                        String value = row.getString(name);
                        value = value.replace("\\", "\\\\");
                        value = value.replace("$", "\\$");
                        value = value.replace("'", "''");
                        value = value.replace("?", QUESTION_MARK);
                        newSql = newSql.replaceFirst(regex, quote + value + quote);
                    } catch (RuntimeException ex) {
                        log.error("Failed to replace ? in {" + sql + "} with " + name + "="
                                + row.getString(name));
                        throw ex;
                    }
                } else if (column.isTimestampWithTimezone()) {
                    newSql = newSql.replaceFirst(regex, quote + row.getString(name) + quote);
                } else if (type == Types.DATE || type == Types.TIMESTAMP || type == Types.TIME) {
                    Date date = row.getDateTime(name);
                    if (useVariableDates) {
                        long diff = date.getTime() - System.currentTimeMillis();
                        newSql = newSql.replaceFirst(regex, "${curdate" + diff + "}");
                    } else if (type == Types.TIME) {
                        newSql = newSql.replaceFirst(regex,
                                "ts {" + quote + FormatUtils.TIME_FORMATTER.format(date) + quote + "}");
                    } else {
                        newSql = newSql.replaceFirst(regex,
                                "ts {" + quote + FormatUtils.TIMESTAMP_FORMATTER.format(date) + quote + "}");
                    }
                } else if (column.isOfBinaryType()) {
                    byte[] bytes = row.getBytes(name);
                    if (encoding == BinaryEncoding.NONE) {
                        newSql = newSql.replaceFirst(regex, quote + row.getString(name));
                    } else if (encoding == BinaryEncoding.BASE64) {
                        newSql = newSql.replaceFirst(regex,
                                quote + new String(Base64.encodeBase64(bytes)) + quote);
                    } else if (encoding == BinaryEncoding.HEX) {
                        newSql = newSql.replaceFirst(regex, quote
                                + new String(Hex.encodeHex(bytes)) + quote);
                    }
                } else {
                    newSql = newSql.replaceFirst(regex, row.getString(name));
                }
            } else {
                newSql = newSql.replaceFirst(regex, "null");
            }
        }
        
        newSql = newSql.replace(QUESTION_MARK, "?");
        return newSql + getDatabaseInfo().getSqlCommandDelimiter();
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

    protected java.util.Date parseDate(int type, String value, boolean useVariableDates) {
        if (StringUtils.isNotBlank(value)) {
            try {
                boolean useTimestamp = (type == Types.TIMESTAMP)
                        || (type == Types.DATE && getDdlBuilder().getDatabaseInfo()
                                .isDateOverridesToTimestamp());

                if (useVariableDates && value.startsWith("${curdate")) {
                    long time = Long.parseLong(value.substring(10, value.length() - 1));
                    if (value.substring(9, 10).equals("-")) {
                        time *= -1L;
                    }
                    time += System.currentTimeMillis();
                    if (useTimestamp) {
                        return new Timestamp(time);
                    }
                    return new Date(time);
                } else {
                    if (useTimestamp) {
                        try {
                            return Timestamp.valueOf(value);
                        } catch (IllegalArgumentException ex) {
                            return FormatUtils.parseDate(value, FormatUtils.TIMESTAMP_PATTERNS);
                        }
                    } else if (type == Types.TIME) {
                        return FormatUtils.parseDate(value, FormatUtils.TIME_PATTERNS);
                    } else {
                        return FormatUtils.parseDate(value, FormatUtils.TIMESTAMP_PATTERNS);
                    }
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            return null;
        }
    }

    public boolean isLob(int type) {
        return isClob(type) || isBlob(type);
    }

    public boolean isClob(int type) {
        return type == Types.CLOB || type == Types.LONGVARCHAR || type == ColumnTypes.LONGNVARCHAR;
    }

    public boolean isBlob(int type) {
        return type == Types.BLOB || type == Types.BINARY || type == Types.VARBINARY
                || type == Types.LONGVARBINARY || type == -10;
    }

    public List<Column> getLobColumns(Table table) {
        List<Column> lobColumns = new ArrayList<Column>(1);
        Column[] allColumns = table.getColumns();
        for (Column column : allColumns) {
            if (isLob(column.getMappedTypeCode())) {
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

    public Database readDatabaseFromXml(String filePath, boolean alterCaseToMatchDatabaseDefaultCase) {
        InputStream is = null;
        try {
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
                return readDatabaseFromXml(is, alterCaseToMatchDatabaseDefaultCase);
            } else {
                throw new IoException("Could not find the file: %s", filePath);
            }
        } finally {
            IOUtils.closeQuietly(is);
        }
    }        
    
    public void alterCaseToMatchDatabaseDefaultCase(Database database) {
        Table[] tables = database.getTables();
        for (Table table : tables) {
            alterCaseToMatchDatabaseDefaultCase(table);
        }
    }
    
    public void alterCaseToMatchDatabaseDefaultCase(Table table) {
        boolean storesUpperCase = isStoresUpperCaseIdentifiers();
        if (!FormatUtils.isMixedCase(table.getName())) {
            table.setName(storesUpperCase ? table.getName().toUpperCase() : table.getName()
                    .toLowerCase());
        }

        Column[] columns = table.getColumns();
        for (Column column : columns) {
            if (!FormatUtils.isMixedCase(column.getName())) {
                column.setName(storesUpperCase ? column.getName().toUpperCase() : column.getName()
                        .toLowerCase());
            }
        }

        IIndex[] indexes = table.getIndices();
        for (IIndex index : indexes) {
            if (!FormatUtils.isMixedCase(index.getName())) {
                index.setName(storesUpperCase ? index.getName().toUpperCase() : index.getName()
                        .toLowerCase());
            }

            IndexColumn[] indexColumns = index.getColumns();
            for (IndexColumn indexColumn : indexColumns) {
                if (!FormatUtils.isMixedCase(indexColumn.getName())) {
                    indexColumn.setName(storesUpperCase ? indexColumn.getName().toUpperCase()
                            : indexColumn.getName().toLowerCase());
                }
            }
        }
    }

    public Database readDatabaseFromXml(InputStream is, boolean alterCaseToMatchDatabaseDefaultCase) {
        InputStreamReader reader = new InputStreamReader(is);
        Database database = DatabaseXmlUtil.read(reader);
        if (alterCaseToMatchDatabaseDefaultCase) {
            alterCaseToMatchDatabaseDefaultCase(database);
        }
        return database;

    }

}
