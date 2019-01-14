package org.jumpmind.db.platform.mysql;

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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.nuodb.NuoDbDdlBuilder;
import org.jumpmind.db.platform.oracle.OracleDdlBuilder;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.util.BasicDataSourceFactory;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.SecurityServiceFactory;

/*
 * Reads a database model from a MySql database.
 */
public class MySqlDdlReader extends AbstractJdbcDdlReader {

    private Boolean mariaDbDriver = null;

    public MySqlDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");
        setDefaultColumnPattern("%");
    }

    @Override
    protected String getResultSetCatalogName() {
        if (isMariaDbDriver()) {
            return "TABLE_SCHEMA";
        } else {
            return super.getResultSetCatalogName();
        }
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        // TODO This needs some more work, since table names can be case
        // sensitive or lowercase
        // depending on the platform (really cute).
        // See http://dev.mysql.com/doc/refman/4.1/en/name-case-sensitivity.html
        // for more info.

        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            determineAutoIncrementFromResultSetMetaData(connection, table,
                    table.getPrimaryKeyColumns());
        }
        return table;
    }

    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        Integer type = (Integer) values.get("DATA_TYPE");
        if ("YEAR".equals(typeName)) {
            // it is safe to map a YEAR to INTEGER
            return Types.INTEGER;
        } else if (typeName != null && typeName.endsWith("TEXT")) {
            String catalog = (String) values.get("TABLE_CAT");
            String tableName = (String) values.get("TABLE_NAME");
            String columnName = (String) values.get("COLUMN_NAME");
            String collation = platform.getSqlTemplate().queryForString("select collation_name from information_schema.columns " +
                    "where table_schema = ? and table_name = ? and column_name = ?",
                    catalog, tableName, columnName);
            
            String convertTextToLobParm = System.getProperty("mysqlddlreader.converttexttolob",
                    "true");
            boolean convertTextToLob = collation != null && collation.endsWith("_bin") && 
                    convertTextToLobParm.equalsIgnoreCase("true");

            if ("LONGTEXT".equals(typeName)) {
                return convertTextToLob ? Types.BLOB : Types.CLOB;
            } else if ("MEDIUMTEXT".equals(typeName)) {
                return convertTextToLob ? Types.BLOB : Types.LONGVARCHAR;
            } else if ("TEXT".equals(typeName)) {
                return convertTextToLob ? Types.BLOB : Types.LONGVARCHAR;
            } else if ("TINYTEXT".equals(typeName)) {
                return convertTextToLob ? Types.BLOB : Types.LONGVARCHAR;
            }
            return super.mapUnknownJdbcTypeForColumn(values);
        } else if (type != null && type == Types.OTHER) {
            return Types.LONGVARCHAR;
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);

        // MySQL converts illegal date/time/timestamp values to
        // "0000-00-00 00:00:00", but this
        // is an illegal ISO value, so we replace it with NULL
        if ((column.getMappedTypeCode() == Types.TIMESTAMP)
                && "0000-00-00 00:00:00".equals(column.getDefaultValue())) {
            column.setDefaultValue(null);
        }
        // make sure the defaultvalue is null when an empty is returned.
        if ("".equals(column.getDefaultValue())) {
            column.setDefaultValue(null);
        }

        if (column.getJdbcTypeName().equalsIgnoreCase(TypeMap.POINT) ||
                column.getJdbcTypeName().equalsIgnoreCase(TypeMap.LINESTRING) ||
                column.getJdbcTypeName().equalsIgnoreCase(TypeMap.POLYGON) ) {
            column.setJdbcTypeName(TypeMap.GEOMETRY);
        }
        
        if (column.getJdbcTypeName().equalsIgnoreCase("enum")) {
            ISqlTemplate template = platform.getSqlTemplate();
            // Version 8 populates TABLE_CAT, all others populate TABLE_SCHEMA
            // But historically, the metaData.getCatalog() was used to provide the value for the query
            
            // Query for version 5.5, 5.6, and 5.7
            String unParsedEnums = template.queryForString("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS"
                    + " WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",
                    //metaData.getCatalog(),
                    //(String) values.get("TABLE_CAT"),
                    (String) values.get("TABLE_SCHEMA"),
                    (String) values.get("TABLE_NAME"), column.getName());
            if(unParsedEnums == null) {
            	// Query for version 8.0
            	unParsedEnums = template.queryForString("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS"
                        + " WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",
                        //metaData.getCatalog(),
                        (String) values.get("TABLE_CAT"),
                        //(String) values.get("TABLE_SCHEMA"),
                        (String) values.get("TABLE_NAME"), column.getName());
            	if(unParsedEnums == null) {
            		// Query originally used
            		unParsedEnums = template.queryForString("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS"
                            + " WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?",
                            metaData.getCatalog(),
                            //(String) values.get("TABLE_CAT"),
                            //(String) values.get("TABLE_SCHEMA"),
                            (String) values.get("TABLE_NAME"), column.getName());
            	}
            }
            if (unParsedEnums != null) {
                unParsedEnums = unParsedEnums.trim();
                if (unParsedEnums.startsWith("(")) {
                    unParsedEnums = unParsedEnums.substring(1);
                    if (unParsedEnums.endsWith(")")) {
                        unParsedEnums = unParsedEnums.substring(0, unParsedEnums.length()-1);
                    }
                }                
                
                String[] parsedEnums = unParsedEnums.split(",");
                for (int i = 0; i < parsedEnums.length; i++) {
                    String parsedEnum = parsedEnums[i];
                    if (parsedEnum.startsWith("'")) {
                        parsedEnum = parsedEnum.substring(1);
                        if (parsedEnum.endsWith("'")) {
                            parsedEnum = parsedEnum.substring(0, parsedEnum.length() - 1);
                        }
                    }
                        
                    parsedEnums[i] = parsedEnum;
                }
                
//                column.setEnumValues(parsedEnums);
                column.getPlatformColumns().get(platform.getName()).setEnumValues(parsedEnums);
            }
        }
        return column;
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        // MySql defines a unique index "PRIMARY" for primary keys
        return "PRIMARY".equals(index.getName());
    }

    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, IIndex index) {
        // MySql defines a non-unique index of the same name as the fk
        return getPlatform().getDdlBuilder().getForeignKeyName(table, fk).equals(index.getName());
    }

    protected boolean isMariaDbDriver() {
        if (mariaDbDriver == null) {
            mariaDbDriver = "mariadb-jdbc".equals(getPlatform().getSqlTemplate().getDriverName());
        }
        return mariaDbDriver;
    }

    @Override
    protected Collection<ForeignKey> readForeignKeys(Connection connection,
            DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        if (!isMariaDbDriver()) {
            return super.readForeignKeys(connection, metaData, tableName);
        } else {
            Map<String, ForeignKey> fks = new LinkedHashMap<String, ForeignKey>();
            ResultSet fkData = null;
            try {
                fkData = metaData.getForeignKeys(tableName);
                while (fkData.next()) {
                    int count = fkData.getMetaData().getColumnCount();
                    Map<String, Object> values = new HashMap<String, Object>();
                    for (int i = 1; i <= count; i++) {
                        values.put(fkData.getMetaData().getColumnName(i), fkData.getObject(i));
                    }
                    String fkName = (String) values.get("CONSTRAINT_NAME");
                    ForeignKey fk = (ForeignKey) fks.get(fkName);

                    if (fk == null) {
                        fk = new ForeignKey(fkName);
                        fk.setForeignTableName((String) values.get("REFERENCED_TABLE_NAME"));
                        fks.put(fkName, fk);
                    }

                    Reference ref = new Reference();

                    ref.setForeignColumnName((String) values.get("REFERENCED_COLUMN_NAME"));
                    ref.setLocalColumnName((String) values.get("COLUMN_NAME"));
                    if (values.containsKey("POSITION_IN_UNIQUE_CONSTRAINT")) {
                        ref.setSequenceValue(((Number) values.get("POSITION_IN_UNIQUE_CONSTRAINT"))
                                .intValue());
                    }
                    fk.addReference(ref);
                }
            } finally {
                close(fkData);
            }
            return fks.values();
        }
    }
    
    public List<Trigger> getTriggers(final String catalog, final String schema,
			final String tableName) {
    	
    	List<Trigger> triggers = new ArrayList<Trigger>();
    	
    	log.debug("Reading triggers for: " + tableName);
		JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
				.getSqlTemplate();
		
		String sql = "SELECT "
						+ "TRIGGER_NAME, "
						+ "TRIGGER_SCHEMA, "
						+ "TRIGGER_CATALOG, "
						+ "EVENT_MANIPULATION AS TRIGGER_TYPE, "
						+ "EVENT_OBJECT_TABLE AS TABLE_NAME, "
						+ "EVENT_OBJECT_SCHEMA AS TABLE_SCHEMA, "
						+ "EVENT_OBJECT_CATALOG AS TABLE_CATALOG, "
						+ "TRIG.* "
					+ "FROM INFORMATION_SCHEMA.TRIGGERS AS TRIG "
					+ "WHERE EVENT_OBJECT_TABLE=? and EVENT_OBJECT_SCHEMA=? ;";
    	triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
			public Trigger mapRow(Row row) {
				Trigger trigger = new Trigger();
				trigger.setName(row.getString("TRIGGER_NAME"));
				trigger.setCatalogName(row.getString("TRIGGER_CATALOG"));
				trigger.setSchemaName(row.getString("TRIGGER_SCHEMA"));
				trigger.setTableName(row.getString("TABLE_NAME"));
				trigger.setEnabled(true);
				String triggerType = row.getString("TRIGGER_TYPE");
				if (triggerType.equals("DELETE")
						|| triggerType.equals("INSERT")
						|| triggerType.equals("UPDATE")) {
					trigger.setTriggerType(TriggerType.valueOf(triggerType));
				}
				trigger.setMetaData(row);
				return trigger;
			}
		}, tableName, catalog);
    	
    	for (final Trigger trigger : triggers) {
    		String name = trigger.getName();
    		String sourceSql = "SHOW CREATE TRIGGER "+ catalog + "." + name;
    		sqlTemplate.query(sourceSql, new ISqlRowMapper<Trigger>() {
    			public Trigger mapRow(Row row) {
    				trigger.setSource(row.getString("SQL Original Statement"));
    				return trigger;
    			}
    		});
    	}
    	
    	return triggers;
    }
    
    public static void main(String[] args) throws SQLException {
//    	mariadb.db.driver=org.mariadb.jdbc.Driver
//		mariadb.db.user=root
//		mariadb.db.password=admin
//		mariadb.root.db.url=jdbc:mysql://localhost/SymmetricRoot?tinyInt1isBit=false
//		mariadb.server.db.url=jdbc:mysql://localhost/SymmetricRoot?tinyInt1isBit=false
//		mariadb.client.db.url=jdbc:mysql://localhost/SymmetricClient?tinyInt1isBit=false
    	TypedProperties properties = new TypedProperties();
    	properties.put("db.driver", "org.mariadb.jdbc.Driver");
//    	properties.put("db.driver", "com.mysql.jdbc.Driver");
    	properties.put("db.user", "root");
    	properties.put("db.password", "my-secret-pw");
    	properties.put("db.url", "jdbc:mysql://localhost:3306/phil?tinyInt1isBit=false");
    	Connection connection = null;
    	MySqlDdlReader reader = new MySqlDdlReader(
    			new MySqlDatabasePlatform(BasicDataSourceFactory.create(properties, SecurityServiceFactory.create()),
    					new SqlTemplateSettings()));
    	Database database = reader.getDatabase(connection);
    	Table[] tables = database.getTables();
    	Table table = tables[0];
    	MySqlDdlBuilder ddlBuilder = new MySqlDdlBuilder();
    	String ddl = ddlBuilder.createTable(table);
        System.out.println(ddl);
        OracleDdlBuilder oDdlBuilder = new OracleDdlBuilder();
        System.out.println(oDdlBuilder.createTable(table));
        System.out.println(new NuoDbDdlBuilder().createTable(table));
    }

}
