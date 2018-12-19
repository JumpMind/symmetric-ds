package org.jumpmind.db.platform.h2;

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

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.MetaDataColumnDescriptor;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;

/*
 * Reads a database model from a H2 database. From patch <a
 * href="https://issues.apache.org/jira/browse/DDLUTILS-185"
 * >https://issues.apache.org/jira/browse/DDLUTILS-185</a>
 */
public class H2DdlReader extends AbstractJdbcDdlReader {

	public H2DdlReader(IDatabasePlatform platform) {
		super(platform);
		setDefaultCatalogPattern(null);
		setDefaultSchemaPattern(null);
	}

	@Override
	protected Column readColumn(DatabaseMetaDataWrapper metaData,
			Map<String, Object> values) throws SQLException {
		Column column = super.readColumn(metaData, values);
		if (values.get("CHARACTER_MAXIMUM_LENGTH") != null) {
			String maxLength = (String) values.get("CHARACTER_MAXIMUM_LENGTH");
			if (isNotBlank(maxLength)) {
				Integer size = new Integer(maxLength);
				column.setSize(size.toString());
				column.findPlatformColumn(platform.getName()).setSize(size);
			}
		}
		if (values.get("COLUMN_DEFAULT") != null) {
			column.setDefaultValue(values.get("COLUMN_DEFAULT").toString());
		}		
		
		if (values.get("NUMERIC_SCALE") != null && values.get("DECIMAL_DIGITS") != null && ((Integer)values.get("DECIMAL_DIGITS")) == 0 ) {
			int scale = (Integer) values.get("NUMERIC_SCALE");
			column.setScale(scale);
			column.findPlatformColumn(platform.getName()).setDecimalDigits(
					scale);
		}
		if (TypeMap.isTextType(column.getMappedTypeCode())
				&& (column.getDefaultValue() != null)) {
			column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
		}

		String autoIncrement = (String) values.get("IS_AUTOINCREMENT");
		if (autoIncrement != null
				&& "YES".equalsIgnoreCase(autoIncrement.trim())) {
			column.setAutoIncrement(true);
			column.setDefaultValue(null);
		}
		return column;
	}

	@Override
	protected String getResultSetSchemaName() {
		return "TABLE_SCHEMA";
	}

	@Override
	protected String getResultSetCatalogName() {
		return "TABLE_CATALOG";
	}

	@Override
	protected List<MetaDataColumnDescriptor> initColumnsForColumn() {
		List<MetaDataColumnDescriptor> result = super.initColumnsForColumn();
		result.add(new MetaDataColumnDescriptor("COLUMN_DEFAULT", 12));
		result.add(new MetaDataColumnDescriptor("NUMERIC_SCALE", 4,
				new Integer(0)));
		result.add(new MetaDataColumnDescriptor("CHARACTER_MAXIMUM_LENGTH", 12));
		return result;
	}

	@Override
	protected boolean isInternalForeignKeyIndex(Connection connection,
			DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk,
			IIndex index) {
		String name = index.getName();
		return name != null
				&& (name.startsWith(fk.getName()) || name
						.startsWith("CONSTRAINT_INDEX_"));
	}

	@Override
	protected boolean isInternalPrimaryKeyIndex(Connection connection,
			DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
		String name = index.getName();
		return name != null && name.startsWith("PRIMARY_KEY_");
	}
	
	@Override
	public List<Trigger> getTriggers(final String catalog, final String schema,
			final String tableName) throws SqlException {
		
		List<Trigger> triggers = new ArrayList<Trigger>();

		log.debug("Reading triggers for: " + tableName);
		JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
				.getSqlTemplate();
		
		String sql = "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS "
				+ "WHERE TABLE_NAME=? and TRIGGER_SCHEMA=? and TRIGGER_CATALOG=? ;";
		triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
			public Trigger mapRow(Row row) {
				Trigger trigger = new Trigger();
				trigger.setName(row.getString("TRIGGER_NAME"));
				trigger.setCatalogName(row.getString("TRIGGER_CATALOG"));
				trigger.setSchemaName(row.getString("TRIGGER_SCHEMA"));
				trigger.setTableName(row.getString("TABLE_NAME"));
				trigger.setEnabled(true);
				trigger.setSource(row.getString("SQL"));
				row.remove("SQL");
				String triggerType = row.getString("TRIGGER_TYPE");
				if (triggerType.equals("DELETE")
						|| triggerType.equals("INSERT")
						|| triggerType.equals("UPDATE")) {
					trigger.setTriggerType(TriggerType.valueOf(triggerType));
				}
				trigger.setMetaData(row);
				return trigger;
			}
		}, tableName, schema, catalog);

		return triggers;
	}

}
