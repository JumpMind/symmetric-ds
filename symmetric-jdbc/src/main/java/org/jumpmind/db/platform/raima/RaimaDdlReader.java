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
package org.jumpmind.db.platform.raima;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;

public class RaimaDdlReader extends AbstractJdbcDdlReader {

    public RaimaDdlReader(IDatabasePlatform platform){
        super(platform); 
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern(null);        
    }
    
    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {

        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            determineAutoIncrementFromResultSetMetaData(connection, table,
                    table.getColumns());
            table.setCatalog(null);
            
            if (table.getIndexCount() > 0) {
            		Collection<IIndex> nonPkIndices = new ArrayList<IIndex>();
            		
            		for (IIndex index : table.getIndices()) {
            			if (index.getColumnCount() == table.getPrimaryKeyColumnCount()) {
            				int matches = 0;
            				for (IndexColumn indexColumn : index.getColumns()) {
            					for (String pkColName : table.getPrimaryKeyColumnNames()) {
            						if (pkColName.equals(indexColumn.getName())) {
            							matches++;
            						}
            					}
            				}
            				if (matches != index.getColumnCount()) {
            					nonPkIndices.add(index);
            				}
            			}
            			else {
            				nonPkIndices.add(index);
            			}
            		}
            		
            		table.removeAllIndices();
            		table.addIndices(nonPkIndices);
            }
        }
        return table;
    }

    
    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        return column;
    }
    
    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        return ((table.getName()).toUpperCase() + "..PRIMARY_KEY").equals(index.getName());
    }
    
    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, IIndex index) {
        return getPlatform().getDdlBuilder().getForeignKeyName(table, fk).equals(index.getName());
    }

    public List<Trigger> getTriggers(final String catalog, final String schema,
            final String tableName) {
        
        List<Trigger> triggers = new ArrayList<Trigger>();
        
        log.debug("Reading triggers for: " + tableName);
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
                .getSqlTemplate();
        
        String sql = "SELECT "
                        + "TRIGGERNAME, "
                        + "SCHEMA, "
                        + "TRIGGER_TYPE, "
                        + "TABLENAME, "
                        + "TRIG.* "
                    + "FROM SYSTEM.TRIGGERS AS TRIG "
                    + "WHERE TABLENAME=? and SCHEMA=? ;";
        triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
            public Trigger mapRow(Row row) {
                Trigger trigger = new Trigger();
                trigger.setName(row.getString("TRIGGERNAME"));
                trigger.setSchemaName(row.getString("SCHEMA"));
                trigger.setTableName(row.getString("TABLENAME"));
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
                
        return triggers;
    }
    
    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        Integer type = (Integer) values.get("DATA_TYPE");
        if (type != null && type.intValue() == Types.ROWID) {
            return Types.BIGINT;
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }
    
}
