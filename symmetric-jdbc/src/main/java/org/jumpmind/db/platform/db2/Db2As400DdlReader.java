package org.jumpmind.db.platform.db2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;

public class Db2As400DdlReader extends Db2DdlReader {

    public Db2As400DdlReader(IDatabasePlatform platform) {
        super(platform);
    }
    
    @Override
    public List<String> getSchemaNames(String catalog) {
        try {
            return super.getSchemaNames(catalog);
        } catch (Exception ex) {
            ArrayList<String> list = new ArrayList<String>();
            list.add(platform.getSqlTemplate().queryForObject(
                    "select CURRENT SCHEMA from sysibm.sysdummy1", String.class));
            return list;
        }
    }

    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        ResultSet pkData = null;
        HashSet<String> pkColNames = new HashSet<String>();

        try {
            pkData = metaData.getPrimaryKeys(table.getName());
            while (pkData.next()) {
                Map<String, Object> values = readMetaData(pkData, getColumnsForPK());
                pkColNames.add((String) values.get("COLUMN_NAME"));
            }
        } finally {
            if (pkData != null) {
                pkData.close();
            }
        }

        boolean indexMatchesPk = true;
        for (int i = 0; i < index.getColumnCount(); i++) {
            if (!pkColNames.contains(index.getColumn(i).getName())) {
                indexMatchesPk = false;
                break;
            }
        }

        return indexMatchesPk;
    }
    
    @Override
    protected void enhanceTableMetaData(Connection connection, DatabaseMetaDataWrapper metaData,
            Table table) throws SQLException {
        log.debug("about to read additional column data");
        /* DB2 does not return the auto-increment status via the database
         metadata */
        String sql = "SELECT NAME, IDENTITY, DEFAULT, DFTVALUE FROM QSYS2.SYSCOLUMNS WHERE TBNAME=?";
        if (StringUtils.isNotBlank(metaData.getSchemaPattern())) {
            sql = sql + " AND DBNAME=?";
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, table.getName());
            if (StringUtils.isNotBlank(metaData.getSchemaPattern())) {
                pstmt.setString(2, metaData.getSchemaPattern());
            }

            rs = pstmt.executeQuery();
            while (rs.next()) {
                String columnName = rs.getString(1);
                Column column = table.getColumnWithName(columnName);
                if (column != null) {
                    String isIdentity = rs.getString(2);
                    if (isIdentity != null && isIdentity.startsWith("Y")) {
                        column.setAutoIncrement(true);
                        if (log.isDebugEnabled()) {
                            log.debug("Found identity column {} on {}", columnName,
                                    table.getName());
                        }
                    }
                    String hasDefault = rs.getString(3);
                    if (hasDefault != null && hasDefault.startsWith("Y")) {
                        column.setDefaultValue(rs.getString(4));
                    } else {
                        column.setDefaultValue(null);
                    }
                }
            }
        } finally {
            JdbcSqlTemplate.close(rs);
            JdbcSqlTemplate.close(pstmt);
        }
        log.debug("done reading additional column data");
    }

    
    @Override
    protected Collection<IIndex> readIndices(Connection connection, DatabaseMetaDataWrapper metaData, String tableName)
            throws SQLException {
        Map<String, IIndex> indices = new LinkedHashMap<String, IIndex>();
        if (getPlatformInfo().isIndicesSupported()) {
            ResultSet indexData = null;
    
            try {
                indexData = metaData.getIndices(getTableNamePatternForConstraints(tableName), false, false);
                
                Collection<Column> columns = readColumns(metaData, tableName);
                
                while (indexData.next()) {
                    Map<String, Object> values = readMetaData(indexData, getColumnsForIndex());
    
                    String columnName = (String) values.get("COLUMN_NAME");
                    if (hasColumn(columns, columnName)) {
                        readIndex(metaData, values, indices);
                    }
                }
            } finally {
                close(indexData);
            }
        }
        return indices.values();
    }
    
    private boolean hasColumn(Collection<Column> columns, String targetColumn) {
        if (targetColumn == null || columns == null || columns.size() == 0) {
            return false;
        }
        boolean found = false;
        for(Column column : columns) {
            if (targetColumn.equals(column.getName())) {
                found = true;
            }
        }
        return found;
    }
    
    public List<Trigger> getTriggers(final String catalog, final String schema,
            final String tableName) throws SqlException {
        
        List<Trigger> triggers = new ArrayList<Trigger>();

        log.debug("Reading triggers for: " + tableName);
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
                .getSqlTemplate();
        
        String sql = "SELECT "
                        + "TRIGGER_NAME, "
                        + "TRIGGER_SCHEMA, "
                        + "EVENT_OBJECT_TABLE as TABLE_NAME, "
                        + "ACTION_TIMING as TRIGGER_TIME, "
                        + "EVENT_MANIPULATION as TRIGGER_TYPE, "
                        + "CREATED, "
                        + "ENABLED "
                    + "FROM QSYS2.SYSTRIGGERS "
                    + "WHERE EVENT_OBJECT_TABLE=? and EVENT_OBJECT_SCHEMA=?";
        triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
            public Trigger mapRow(Row row) {
                Trigger trigger = new Trigger();
                trigger.setName(row.getString("TRIGGER_NAME"));
                trigger.setSchemaName(row.getString("TRIGGER_SCHEMA"));
                trigger.setTableName(row.getString("TABLE_NAME"));
                trigger.setEnabled(row.getString("ENABLED").equals("Y"));
                trigger.setTriggerType(TriggerType.valueOf(row.getString("TRIGGER_TYPE")));
                trigger.setMetaData(row);
                return trigger;
            }
        }, tableName, schema);
        
        return triggers;
    }
}
