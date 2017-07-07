package org.jumpmind.db.platform.ase;

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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;

/*
 * Reads a database model from a Sybase database.
 */
public class AseDdlReader extends AbstractJdbcDdlReader {

    /* The regular expression pattern for the ISO dates. */
    private Pattern isoDatePattern = Pattern.compile("'(\\d{4}\\-\\d{2}\\-\\d{2})'");

    /* The regular expression pattern for the ISO times. */
    private Pattern isoTimePattern = Pattern.compile("'(\\d{2}:\\d{2}:\\d{2})'");

    public AseDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            // Sybase does not return the auto-increment status via the database
            // metadata
            determineAutoIncrementFromResultSetMetaData(connection, table, table.getColumns());
        }
        return table;
    }

    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.toUpperCase().startsWith("TEXT")) {
            return Types.LONGVARCHAR;
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String,Object> values) throws SQLException {
        Column column = super.readColumn(metaData, values);

        if ((column.getMappedTypeCode() == Types.NUMERIC) && (column.getSizeAsInt() == 18)
                && (column.getScale() == 0)) {
            // Back-mapping to BIGINT
            column.setMappedTypeCode(Types.BIGINT);
        } else if ((column.getMappedTypeCode() == Types.NUMERIC) && (column.getSizeAsInt() == 12)
                && (column.getScale() == 0)) {
            // Back-mapping to INTEGER
            column.setMappedTypeCode(Types.INTEGER);
        } else if (column.getDefaultValue() != null) {
            if (column.getMappedTypeCode() == Types.TIMESTAMP) {
                // Sybase maintains the default values for DATE/TIME jdbc types,
                // so we have to
                // migrate the default value to TIMESTAMP
                Matcher matcher = isoDatePattern.matcher(column.getDefaultValue());
                Timestamp timestamp = null;

                if (matcher.matches()) {
                    timestamp = new Timestamp(Date.valueOf(matcher.group(1)).getTime());
                } else {
                    matcher = isoTimePattern.matcher(column.getDefaultValue());
                    if (matcher.matches()) {
                        timestamp = new Timestamp(Time.valueOf(matcher.group(1)).getTime());
                    }
                }
                if (timestamp != null) {
                    column.setDefaultValue(timestamp.toString());
                }
            } else if (TypeMap.isTextType(column.getMappedTypeCode())) {
                column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
            }
        }
        return column;
    }

    @Override
    protected void readIndex(DatabaseMetaDataWrapper metaData, Map<String,Object> values, Map<String,IIndex> knownIndices)
            throws SQLException {
        if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
            String indexName = (String) values.get("INDEX_NAME");

            // Sometimes, Sybase keeps the delimiter quotes around the index
            // names
            // when returning them in the metadata, so we strip them
            if (indexName != null) {
                String delimiter = getPlatformInfo().getDelimiterToken();

                if ((indexName != null) && indexName.startsWith(delimiter)
                        && indexName.endsWith(delimiter)) {
                    indexName = indexName.substring(delimiter.length(), indexName.length()
                            - delimiter.length());
                    values.put("INDEX_NAME", indexName);
                }
            }
        }
        super.readIndex(metaData, values, knownIndices);
    }

    @Override
    protected Collection<ForeignKey> readForeignKeys(Connection connection, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        // Sybase (or jConnect) does not return the foreign key names, thus we
        // have to
        // read the foreign keys manually from the system tables
        StringBuffer query = new StringBuffer();

        query.append("SELECT refobjs.name, localtables.id, remotetables.name, remotetables.id");
        for (int idx = 1; idx <= 16; idx++) {
            query.append(", refs.fokey");
            query.append(idx);
            query.append(", refs.refkey");
            query.append(idx);
        }
        query.append(" FROM dbo.sysreferences refs, dbo.sysobjects refobjs, dbo.sysobjects localtables, dbo.sysobjects remotetables");
        query.append(" WHERE refobjs.type = 'RI' AND refs.constrid = refobjs.id AND");
        query.append(" localtables.type = 'U' AND refs.tableid = localtables.id AND localtables.name = '");
        query.append(tableName);
        query.append("' AND remotetables.type = 'U' AND refs.reftabid = remotetables.id");

        Statement stmt = connection.createStatement();
        PreparedStatement prepStmt = connection
                .prepareStatement("SELECT name FROM dbo.syscolumns WHERE id = ? AND colid = ?");
        ArrayList<ForeignKey> result = new ArrayList<ForeignKey>();

        try {
            ResultSet fkRs = stmt.executeQuery(query.toString());

            while (fkRs.next()) {
                ForeignKey fk = new ForeignKey(fkRs.getString(1));
                int localTableId = fkRs.getInt(2);
                int remoteTableId = fkRs.getInt(4);

                fk.setForeignTableName(fkRs.getString(3));
                for (int idx = 0; idx < 16; idx++) {
                    short fkColIdx = fkRs.getShort(5 + idx + idx);
                    short pkColIdx = fkRs.getShort(6 + idx + idx);
                    Reference ref = new Reference();

                    if (fkColIdx == 0) {
                        break;
                    }

                    prepStmt.setInt(1, localTableId);
                    prepStmt.setShort(2, fkColIdx);

                    ResultSet colRs = prepStmt.executeQuery();

                    if (colRs.next()) {
                        ref.setLocalColumnName(colRs.getString(1));
                    }
                    colRs.close();

                    prepStmt.setInt(1, remoteTableId);
                    prepStmt.setShort(2, pkColIdx);

                    colRs = prepStmt.executeQuery();

                    if (colRs.next()) {
                        ref.setForeignColumnName(colRs.getString(1));
                    }
                    colRs.close();

                    fk.addReference(ref);
                }
                result.add(fk);
            }

            fkRs.close();
        } finally {
            stmt.close();
            prepStmt.close();
        }

        return result;
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        // We can simply check the sysindexes table where a specific flag is set
        // for pk indexes
        StringBuffer query = new StringBuffer();

        query.append("SELECT name = si.name FROM dbo.sysindexes si, dbo.sysobjects so WHERE so.name = '");
        query.append(table.getName());
        query.append("' AND si.name = '");
        query.append(index.getName());
        query.append("' AND so.id = si.id AND (si.status & 2048) > 0");

        Statement stmt = connection.createStatement();

        try {
            ResultSet rs = stmt.executeQuery(query.toString());
            boolean result = rs.next();

            rs.close();
            return result;
        } finally {
            stmt.close();
        }
    }
    
    @Override
	public List<Trigger> getTriggers(final String catalog, final String schema,
			final String tableName) throws SqlException {
		
		List<Trigger> triggers = new ArrayList<Trigger>();

		log.debug("Reading triggers for: " + tableName);
		JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
				.getSqlTemplate();
		
		String sql = "SELECT "
						+ "trig.name AS trigger_name, "
						+ "trig.id AS trigger_id, "
						+ "tab.name AS table_name, "
						+ "tab.id AS table_id, "
						+ "db.name AS catalog, "
						+ "trig.crdate AS created_on, "
						+ "tab.deltrig AS table_delete_trigger_id, "
						+ "tab.instrig AS table_insert_trigger_id, "
						+ "tab.updtrig AS table_update_trigger_id "
				   + "FROM sysobjects AS trig "
				   + "INNER JOIN sysobjects AS tab "
				   		+ "ON trig.id = tab.deltrig "
				   		+ "OR trig.id = tab.instrig "
				   		+ "OR trig.id = tab.updtrig "
			   		+ "INNER JOIN master.dbo.sysdatabases AS db "
				   		+ "ON db.dbid = db_id() "
				   + "WHERE tab.name = ? AND db.name = ? ";
		triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
			public Trigger mapRow(Row row) {
				Trigger trigger = new Trigger();
				trigger.setName(row.getString("trigger_name"));
				trigger.setTableName(row.getString("table_name"));
				trigger.setCatalogName(row.getString("catalog"));
				trigger.setEnabled(true);
				trigger.setSource("");
				if (row.getString("table_insert_trigger_id")
						.equals(row.getString("trigger_id"))) {
					trigger.setTriggerType(TriggerType.INSERT);
					row.put("trigger_type", "insert");
				} else if (row.getString("table_delete_trigger_id")
						.equals(row.getString("trigger_id"))) {
					trigger.setTriggerType(TriggerType.DELETE);
					row.put("trigger_type", "delete");
				} else if (row.getString("table_update_trigger_id")
						.equals(row.getString("trigger_id"))) {
					trigger.setTriggerType(TriggerType.UPDATE);
					row.put("trigger_type", "update");
				}
				row.remove("table_insert_trigger_id");
				row.remove("table_delete_trigger_id");
				row.remove("table_update_trigger_id");
				trigger.setMetaData(row);
				return trigger;
			}
		}, tableName, catalog);
		
		
		for (final Trigger trigger : triggers) {
			int id = (Integer) trigger.getMetaData().get("trigger_id");
			String sourceSql = "SELECT text "
							 + "FROM syscomments "
							 + "WHERE id = ? "
							 + "ORDER BY colid ";
			sqlTemplate.query(sourceSql, new ISqlRowMapper<Trigger>() {
				public Trigger mapRow(Row row) {
					trigger.setSource(trigger.getSource()+"\n"+row.getString("text"));					
					return trigger;
				}
			}, id);
		}
		
		return triggers;
	}

    @Override
    protected String getTableNamePattern(String tableName) {
        tableName = tableName.replace("_", "\\_");
        tableName = tableName.replace("%", "\\%");
        return tableName;
    }
    
    @Override
    protected StringBuilder appendColumn(StringBuilder query, String identifier) {
        query.append("\"");
        query.append(identifier);
        query.append("\"");
        return query;
    }
}
