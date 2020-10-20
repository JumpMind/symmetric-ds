package org.jumpmind.db.platform.tibero;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.ForeignKey.ForeignKeyAction;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;

public class TiberoDdlReader extends AbstractJdbcDdlReader {

    /* The regular expression pattern for the Tibero conversion of ISO dates. */
    private Pattern TiberoIsoDatePattern;

    /* The regular expression pattern for the Tibero conversion of ISO times. */
    private Pattern TiberoIsoTimePattern;

    /*
     * The regular expression pattern for the Tibero conversion of ISO
     * timestamps.
     */
    private Pattern TiberoIsoTimestampPattern;

    public TiberoDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");

        TiberoIsoDatePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD'\\)");
        TiberoIsoTimePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'HH24:MI:SS'\\)");
        TiberoIsoTimestampPattern = Pattern
                .compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD HH24:MI:SS'\\)");
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        /*
         * Tibero 10 added the recycle bin which contains dropped database
         * objects not yet purged Since we don't want entries from the recycle
         * bin, we filter them out
         */
        boolean tableHasBeenDeleted = isTableInRecycleBin(connection, values);

        /*
         * System tables are in the system schema
         */
        String schema = (String) values.get(getResultSetSchemaName());

        if (!tableHasBeenDeleted && !"SYSTEM".equals(schema)) {

            Table table = super.readTable(connection, metaData, values);
            if (table != null) {
                determineAutoIncrementColumns(connection, table);
            }

            return table;
        } else {
            return null;
        }
    }

    protected boolean isTableInRecycleBin(Connection connection, Map<String, Object> values)
            throws SQLException {
        String tablename = (String)values.get("TABLE_NAME");
        return StringUtils.isNotBlank(tablename) && tablename.toLowerCase().startsWith("bin$");
    }

    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.startsWith("DATE")) {
            return Types.DATE;
        } else if (typeName != null && typeName.startsWith("TIMESTAMP")
                && !typeName.endsWith("TIME ZONE")) {
            // This is for Tibero's TIMESTAMP(9)
            return Types.TIMESTAMP;
        } else if (typeName != null && typeName.startsWith("TIMESTAMP")
                && typeName.endsWith("WITH TIME ZONE")) {
            return ColumnTypes.TIBERO_TIMESTAMPTZ;
        } else if (typeName != null && typeName.startsWith("TIMESTAMP")
                && typeName.endsWith("WITH LOCAL TIME ZONE")) {
            return ColumnTypes.TIBERO_TIMESTAMPLTZ;
        } else if (typeName != null && typeName.startsWith("NVARCHAR")) {
            // This is for Tibero's NVARCHAR type
            return Types.VARCHAR;
        } else if (typeName != null && typeName.startsWith("LONGNVARCHAR")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.startsWith("NCHAR")) {
            return Types.CHAR;
        } else if (typeName != null && typeName.startsWith("XML")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.endsWith("CLOB")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.startsWith("BINARY_FLOAT")) {
            return Types.FLOAT;
        } else if (typeName != null && typeName.startsWith("BINARY_DOUBLE")) {
            return Types.DOUBLE;
        } else if (typeName != null && typeName.startsWith("BFILE")) {
            return Types.VARCHAR;
        } else if (typeName != null && typeName.startsWith("INTERVAL")) {
            return Types.VARCHAR;            
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        if (column.getMappedTypeCode() == Types.NUMERIC) {
            // PlatformColumn platformColumn = column.getPlatformColumns().get(platform.getName());
            //if (platformColumn.getDecimalDigits() == 0 && column.getSizeAsInt() == 15) {
            //    column.setSize("15");
            //}
            if (column.getScale() == 0) {
                if (column.getSizeAsInt() == 3) {
                    column.setMappedTypeCode(Types.TINYINT);
                } else if (column.getSizeAsInt() <= 22) {
                    column.setMappedTypeCode(Types.INTEGER);
                } else if (column.getSizeAsInt() == 38) {
                    column.setMappedTypeCode(Types.BIGINT);
                }
            } 
            /*
            else {
                if (column.getSizeAsInt() <= 63) {
                    column.setMappedTypeCode(Types.REAL);
                } else {
                    column.setMappedTypeCode(Types.DOUBLE);
                }
            }*/
                
        } else if (column.getMappedTypeCode() == Types.FLOAT) {
            // Same for REAL, FLOAT, DOUBLE PRECISION, which all back-map to
            // FLOAT but with
            // different sizes (63 for REAL, 126 for FLOAT/DOUBLE PRECISION)
            switch (column.getSizeAsInt()) {
                case 63:
                    column.setMappedTypeCode(Types.REAL);
                    break;
                case 126:
                    column.setMappedTypeCode(Types.DOUBLE);
                    break;
            }
        } else if ((column.getMappedTypeCode() == Types.DATE)
                || (column.getMappedTypeCode() == Types.TIMESTAMP)) {
            // we also reverse the ISO-format adaptation, and adjust the default
            // value to timestamp
            if (column.getDefaultValue() != null) {
                Timestamp timestamp = null;

                Matcher matcher = TiberoIsoTimestampPattern.matcher(column.getDefaultValue());

                if (matcher.matches()) {
                    String timestampVal = matcher.group(1);
                    timestamp = Timestamp.valueOf(timestampVal);
                } else {
                    matcher = TiberoIsoDatePattern.matcher(column.getDefaultValue());
                    if (matcher.matches()) {
                        String dateVal = matcher.group(1);
                        timestamp = new Timestamp(Date.valueOf(dateVal).getTime());
                    } else {
                        matcher = TiberoIsoTimePattern.matcher(column.getDefaultValue());
                        if (matcher.matches()) {
                            String timeVal = matcher.group(1);

                            timestamp = new Timestamp(Time.valueOf(timeVal).getTime());
                        }
                    }
                }
                if (timestamp != null) {
                    column.setDefaultValue(timestamp.toString());
                }
            }
        } else if (TypeMap.isTextType(column.getMappedTypeCode())) {
            String defaultValue = column.getDefaultValue();
            if (isNotBlank(defaultValue) && defaultValue.startsWith("('") && defaultValue.endsWith("')")) {
                defaultValue = defaultValue.substring(2, defaultValue.length()-2);
            }
            column.setDefaultValue(unescape(defaultValue, "'", "''"));
        }
        return column;
    }

//    private boolean isColumnInteger(String tableName, String columnName) {
//        return (platform.getSqlTemplate().queryForInt(
//                "select case when data_precision is null and data_scale=0 then 1 else 0 end " +
//                "from all_tab_columns where table_name=? and column_name=?", tableName, columnName) == 1);
//    }

    /*
     * Helper method that determines the auto increment status using Firebird's
     * system tables.
     *
     * @param table The table
     */
    protected void determineAutoIncrementColumns(Connection connection, Table table)
            throws SQLException {
        Column[] columns = table.getColumns();

        for (int idx = 0; idx < columns.length; idx++) {
            columns[idx].setAutoIncrement(isAutoIncrement(connection, table, columns[idx]));
        }
    }

    /*
     * Tries to determine whether the given column is an identity column.
     *
     * @param table The table
     *
     * @param column The column
     *
     * @return <code>true</code> if the column is an identity column
     */
    protected boolean isAutoIncrement(Connection connection, Table table, Column column)
            throws SQLException {
        // TODO: For now, we only check whether there is a sequence & trigger as
        // generated by DdlUtils
        // But once sequence/trigger support is in place, it might be possible
        // to 'parse' the
        // trigger body (via SELECT trigger_name, trigger_body FROM
        // user_triggers) in order to
        // determine whether it fits our auto-increment definition
        PreparedStatement prepStmt = null;
        IDdlBuilder builder = getPlatform().getDdlBuilder();
        String triggerName = builder.getConstraintName(TiberoDdlBuilder.PREFIX_TRIGGER, table,
                column.getName(), null);
        String seqName = builder.getConstraintName(TiberoDdlBuilder.PREFIX_SEQUENCE, table,
                column.getName(), null);

        if (!getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
            triggerName = triggerName.toUpperCase();
            seqName = seqName.toUpperCase();
        }
        try {
            prepStmt = connection
                    .prepareStatement("SELECT * FROM user_triggers WHERE trigger_name = ?");
            prepStmt.setString(1, triggerName);

            ResultSet resultSet = prepStmt.executeQuery();

            if (!resultSet.next()) {
                resultSet.close();
                return false;
            }
            // we have a trigger, so lets check the sequence
            prepStmt.close();

            prepStmt = connection
                    .prepareStatement("SELECT * FROM user_sequences WHERE sequence_name = ?");
            prepStmt.setString(1, seqName);

            resultSet = prepStmt.executeQuery();
            boolean resultFound = resultSet.next();
            resultSet.close();
            return resultFound;
        } finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
        }
    }

    @Override
    protected Collection<IIndex> readIndices(Connection connection,
            DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        // Tibero bug 4999817 causes a table analyze to execute in response to a
        // call to
        // DatabaseMetaData#getIndexInfo.
        // The bug is fixed in driver version 10.2.0.4. The bug is present in at
        // least
        // driver versions 10.2.0.1.0, 10.1.0.2.0, and 9.2.0.5.
        // To avoid this bug, we will access user_indexes view.
        // This also allows us to filter system-generated indices which are
        // identified by either
        // having GENERATED='Y' in the query result, or by their index names
        // being equal to the
        // name of the primary key of the table

        StringBuilder query = new StringBuilder();

        query.append("SELECT a.INDEX_NAME, a.INDEX_TYPE, a.UNIQUENESS, b.COLUMN_NAME, b.COLUMN_POSITION FROM USER_INDEXES a, USER_IND_COLUMNS b WHERE ");
        query.append("a.TABLE_NAME=? AND a.GENERATED_BY_SYSTEM=? AND a.TABLE_TYPE=? AND a.TABLE_NAME=b.TABLE_NAME AND a.INDEX_NAME=b.INDEX_NAME AND ");
        query.append("a.INDEX_NAME NOT IN (SELECT DISTINCT c.CONSTRAINT_NAME FROM USER_CONSTRAINTS c WHERE c.CONSTRAINT_TYPE=? AND c.TABLE_NAME=a.TABLE_NAME");
        if (metaData.getSchemaPattern() != null) {
            query.append(" AND c.OWNER LIKE ?) AND a.TABLE_OWNER LIKE ?");
        } else {
            query.append(")");
        }

        Map<String, IIndex> indices = new LinkedHashMap<String, IIndex>();
        PreparedStatement stmt = null;

        try {
            stmt = connection.prepareStatement(query.toString());
            stmt.setString(
                    1,
                    getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? tableName : tableName
                            .toUpperCase());
            stmt.setString(2, "N");
            stmt.setString(3, "TABLE");
            stmt.setString(4, "P");
            if (metaData.getSchemaPattern() != null) {
                stmt.setString(5, metaData.getSchemaPattern().toUpperCase());
                stmt.setString(6, metaData.getSchemaPattern().toUpperCase());
            }

            ResultSet rs = stmt.executeQuery();
            Map<String, Object> values = new HashMap<String, Object>();

            while (rs.next()) {
                String name = rs.getString(1);
                String type = rs.getString(2);
                // Only read in normal Tibero indexes
                if (type.startsWith("NORMAL")) {
                    values.put("INDEX_TYPE", Short.valueOf(DatabaseMetaData.tableIndexOther));
                    values.put("INDEX_NAME", name);
                    values.put("NON_UNIQUE",
                            "UNIQUE".equalsIgnoreCase(rs.getString(3)) ? Boolean.FALSE
                                    : Boolean.TRUE);
                    values.put("COLUMN_NAME", rs.getString(4));
                    values.put("ORDINAL_POSITION", Short.valueOf(rs.getShort(5)));

                    readIndex(metaData, values, indices);
                } else if (log.isDebugEnabled()) {
                    log.debug("Skipping index " + name + " of type " + type);
                }
            }

            rs.close();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return indices.values();
    }
    
    @Override
    protected String getTableNamePattern(String tableName) {
        /*
         * When looking up a table definition, Tibero treats underscore (_) in
         * the table name as a wildcard, so it needs to be escaped, or you'll
         * get back column names for more than one table. Example:
         * DatabaseMetaData.metaData.getColumns(null, null, "SYM\\_NODE", null)
         */
       return String.format("%s", tableName).replaceAll("\\_", "/_");
    }
    
    public List<String> getTableNames(final String catalog, final String schema,
            final String[] tableTypes) {
        
        List<String> tableNames = new ArrayList<String>();
        
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplate();
        
        StringBuilder sql = new StringBuilder("select TABLE_NAME from ALL_TABLES");
        Object[] params = null;
        if (isNotBlank(schema)) {
            sql.append(" where OWNER=?");
            params = new Object[] { schema };
        }
        tableNames = sqlTemplate.query(sql.toString(), new ISqlRowMapper<String>() {
            public String mapRow(Row row) {
                return row.getString("TABLE_NAME");
            }
        }, params);
        
        return tableNames;
    }
    
    public List<Trigger> getTriggers(final String catalog, final String schema,
            final String tableName) throws SqlException {
        
        List<Trigger> triggers = new ArrayList<Trigger>();

        log.debug("Reading triggers for: " + tableName);
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
                .getSqlTemplate();
        
        String sql = "SELECT * FROM ALL_TRIGGERS "
                + "WHERE TABLE_NAME=? and OWNER=?";
        triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
            public Trigger mapRow(Row row) {
                Trigger trigger = new Trigger();
                trigger.setName(row.getString("TRIGGER_NAME"));
                trigger.setSchemaName(row.getString("OWNER"));
                trigger.setTableName(row.getString("TABLE_NAME"));
                trigger.setEnabled(Boolean.valueOf(row.getString("STATUS")));
                trigger.setSource("create ");
                String triggerType = row.getString("TRIGGERING_EVENT");
                if (triggerType.equals("DELETE")
                        || triggerType.equals("INSERT")
                        || triggerType.equals("UPDATE")) {
                    trigger.setTriggerType(TriggerType.valueOf(triggerType));
                }
                trigger.setMetaData(row);
                return trigger;
            }
        }, tableName, schema);
        
        for (final Trigger trigger : triggers) {
            String name = trigger.getName();
            String sourceSql = "select TEXT from all_source "
                             + "where NAME=? order by LINE ";
            sqlTemplate.query(sourceSql, new ISqlRowMapper<Trigger>() {
                public Trigger mapRow(Row row) {
                    trigger.setSource(trigger.getSource()+"\n"+row.getString("TEXT"));;
                    return trigger;
                }
            }, name);
        }
        
        return triggers;
    }
    
    // Tibero does not support on update actions
    @Override
    protected void readForeignKeyUpdateRule(Map<String, Object> values, ForeignKey fk) {
        fk.setOnUpdateAction(ForeignKeyAction.NOACTION);
    }
}
