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

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.util.BinaryEncoding;

/*
 * A platform encapsulates the database-related functionality such as performing queries
 * and manipulations. It also contains functionality to read and build DDL to create and
 * alter database tables.
 */
public interface IDatabasePlatform {

    /**
     * @see DatabaseNamesConstants
     * @return a constant that represents this database type from {@link DatabaseNamesConstants}
     */
    public String getName();

    /**
     * Returns information about this platform.
     *
     * @return The info object
     */
    public DatabaseInfo getDatabaseInfo();

    /**
     * Returns a new ddl builder for the this platform.
     */
    public IDdlBuilder getDdlBuilder();

    /**
     * Returns the ddl reader (which reads a database model from a live
     * database) for this platform.
     *
     * @return The model reader
     */
    public IDdlReader getDdlReader();

    public ISqlTemplate getSqlTemplate();

    /**
     * The amount of time table metadata will be cached when using {@link IDatabasePlatform#getT
     * @param clearCacheModelTimeoutInMs
     */
    public void setClearCacheModelTimeoutInMs(long clearCacheModelTimeoutInMs);

    public long getClearCacheModelTimeoutInMs();

    public String getDefaultSchema();

    public String getDefaultCatalog();

    /**
     * Reads the database model from the live database to which the given
     * connection is pointing.
     */
    public Database readDatabase(String catalog, String schema, String[] tableTypes);

    public Database readFromDatabase(Table... tables);
    
    public Table readTableFromDatabase(String catalogName, String schemaName, String tablename);
    
    public void resetCachedTableModel();

    public Table getTableFromCache(String tableName, boolean forceReread);

    public Table getTableFromCache(String catalogName, String schemaName, String tableName,
            boolean forceReread);

    public void createDatabase(Database targetDatabase, boolean dropTablesFirst,
            boolean continueOnError);

    public void createTables(boolean dropTablesFirst,
            boolean continueOnError, Table... tables);

    public void alterDatabase(Database desiredDatabase, boolean continueOnError);

    public void alterTables(boolean continueOnError, Table... desiredTables);

    public void dropDatabase(Database database, boolean continueOnError);
    
    public void dropTables(boolean continueOnError, Table...tables);

    public DmlStatement createDmlStatement(DmlType dmlType, Table table, String textColumnExpression);

    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, boolean[] nullKeyValues, String textColumnExpression);
    
    public DmlStatement createDmlStatement(DmlType dmlType, String catalogName, String schemaName,
            String tableName, Column[] keys, Column[] columns, boolean[] nullKeyValues, String textColumnExpression, 
            boolean namedParameters);    

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData);

    public Object[] getObjectValues(BinaryEncoding encoding, Table table, String[] columnNames,
            String[] values);

    public Object[] getObjectValues(BinaryEncoding encoding, Table table, String[] columnNames,
            String[] values, boolean useVariableDates, boolean fitToColumn);

    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData, boolean useVariableDates, boolean fitToColumn);

    public String[] getStringValues(BinaryEncoding encoding, Column[] metaData, Row row, boolean useVariableDates, boolean indexByPosition);

    public Database readDatabaseFromXml(String filePath, boolean alterCaseToMatchDatabaseDefaultCase);

    public Database readDatabaseFromXml(InputStream in, boolean alterCaseToMatchDatabaseDefaultCase);

    public String[] alterCaseToMatchDatabaseDefaultCase(String[] values); 
    
    public String alterCaseToMatchDatabaseDefaultCase(String values);

    public void alterCaseToMatchDatabaseDefaultCase(Table table);

    public void alterCaseToMatchDatabaseDefaultCase(Table... tables);

    public void alterCaseToMatchDatabaseDefaultCase(Database database);
    
    public void prefixDatabase(String prefix, Database targetTables);

    public boolean isLob(int type);

    public boolean isClob(int type);

    public boolean isBlob(int type);

    public List<Column> getLobColumns(Table table);

    public Map<String, String> getSqlScriptReplacementTokens();

    public String scrubSql(String sql);

    public boolean isStoresLowerCaseIdentifiers();

    public boolean isStoresUpperCaseIdentifiers();

    public boolean isStoresMixedCaseQuotedIdentifiers();

    public <T> T getDataSource();

    public void setMetadataIgnoreCase(boolean value);

    public boolean isMetadataIgnoreCase();

    public java.util.Date parseDate(int type, String value, boolean useVariableDates);
    
    public Map<String, String> parseQualifiedTableName(String tableName);

    public Table makeAllColumnsPrimaryKeys(Table table);
    
    public boolean canColumnBeUsedInWhereClause(Column column);
    
    public void makePlatformSpecific(Database database);
    
}
