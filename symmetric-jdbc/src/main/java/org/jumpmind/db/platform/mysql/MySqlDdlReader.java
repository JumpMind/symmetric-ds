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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Reference;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;

/*
 * Reads a database model from a MySql database.
 */
public class MySqlDdlReader extends AbstractJdbcDdlReader {

    private Boolean mariaDbDriver = null;

    public MySqlDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern(null);
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
        if ("YEAR".equals(typeName)) {
            // it is safe to map a YEAR to INTEGER
            return Types.INTEGER;
        } else if ("LONGTEXT".equals(typeName)) {
            return Types.CLOB;
        } else if ("MEDIUMTEXT".equals(typeName)) {
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

        if (column.getJdbcTypeName().equalsIgnoreCase(TypeMap.POINT)) {
            column.setJdbcTypeName(TypeMap.GEOMETRY);
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
}
