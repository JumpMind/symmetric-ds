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
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

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
        }
        return table;
    }

    /*
    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        return column;
    }
    */

    /*
    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        return ((table.getName()).toUpperCase() + "..PRIMARY_KEY").equals(index.getName());
    }
    */

    /*
    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, IIndex index) {
        return getPlatform().getDdlBuilder().getForeignKeyName(table, fk).equals(index.getName());
    }
    */

    /*
    @Override
    protected Collection<ForeignKey> readForeignKeys(Connection connection,
            DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {

            Map<String, ForeignKey> fks = new LinkedHashMap<String, ForeignKey>();
            ResultSet fkData = null;
           
            try {
                PreparedStatement ps = connection.prepareStatement(
                        "SELECT f2.FIELD AS REFERENCED_COLUMN_NAME, t2.TABLENAME AS REFERENCED_TABLE_NAME, f.FIELD AS COLUMN_NAME, \"POSITION\", FOREIGNKEYNAME AS CONSTRAINT_NAME " +
                        "FROM \"SYSTEM\".FOREIGNKEYS AS fk "+
                        "INNER JOIN SYSTEM.TABLES AS t ON t.TABLEID = fk.FOREIGNTABLEID "+
                        "INNER JOIN SYSTEM.TABLES AS t2 ON t2.TABLEID = fk.PRIMARYTABLEID "+
                        "INNER JOIN SYSTEM.FIELDS AS f ON f.FIELDID = fk.FOREIGNFIELDID AND f.TABLENAME = t.TABLENAME AND f.\"SCHEMA\"= t.\"SCHEMA\" " + 
                        "INNER JOIN SYSTEM.FIELDS AS f2 ON f2.FIELDID = fk.PRIMARYFIELDID AND f2.TABLENAME = t2.TABLENAME AND f2.\"SCHEMA\" = t2.\"SCHEMA\" " +
                        "WHERE t.TABLENAME = ?");
                ps.setString(1, tableName);
                
                fkData = ps.executeQuery();
                while (fkData.next()) {
              
                    String fkName = fkData.getString(5);
                    ForeignKey fk = (ForeignKey) fks.get(fkName);

                    if (fk == null) {
                        fk = new ForeignKey(fkName);
                        fk.setForeignTableName(fkData.getString(2));
                        fks.put(fkName, fk);
                    }

                    Reference ref = new Reference();

                    ref.setForeignColumnName(fkData.getString(1));
                    ref.setLocalColumnName(fkData.getString(3));
                    ref.setSequenceValue(fkData.getInt(4));
                    
                    fk.addReference(ref);
                }
            } finally {
                close(fkData);
            }
            return fks.values();
    }
    */
    
    /*
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
        return super.mapUnknownJdbcTypeForColumn(values);
    }
    */
}
