package org.jumpmind.symmetric.jdbc.db.oracle;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Index;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.jdbc.db.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.jdbc.db.IJdbcPlatform;
import org.jumpmind.symmetric.jdbc.db.JdbcModelReader;
import org.jumpmind.symmetric.jdbc.sql.JdbcSqlConnection;

public class OracleJdbcModelReader extends JdbcModelReader {

    /** The regular expression pattern for the Oracle conversion of ISO dates. */
    private Pattern oracleIsoDatePattern;

    /** The regular expression pattern for the Oracle conversion of ISO times. */
    private Pattern oracleIsoTimePattern;

    /**
     * The regular expression pattern for the Oracle conversion of ISO
     * timestamps.
     */
    private Pattern oracleIsoTimestampPattern;

    /**
     * Creates a new model reader for Oracle 8 databases.
     * 
     * @param platform
     *            The platform that this model reader belongs to
     */
    public OracleJdbcModelReader(IJdbcPlatform platform, DataSource dataSource) {
        super(platform, dataSource);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");

        oracleIsoDatePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD'\\)");
        oracleIsoTimePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'HH24:MI:SS'\\)");
        oracleIsoTimestampPattern = Pattern
                .compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD HH24:MI:SS'\\)");
    }

    /**
     * {@inheritDoc}
     */
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);

        if (column.getDefaultValue() != null) {
            // Oracle pads the default value with spaces
            column.setDefaultValue(column.getDefaultValue().trim());
        }
        if (column.getTypeCode() == Types.DECIMAL) {
            // We're back-mapping the NUMBER columns returned by Oracle
            // Note that the JDBC driver returns DECIMAL for these NUMBER
            // columns
            switch (column.getSizeAsInt()) {
            case 1:
                if (column.getScale() == 0) {
                    column.setTypeCode(Types.BIT);
                }
                break;
            case 3:
                if (column.getScale() == 0) {
                    column.setTypeCode(Types.TINYINT);
                }
                break;
            case 5:
                if (column.getScale() == 0) {
                    column.setTypeCode(Types.SMALLINT);
                }
                break;
            case 18:
                column.setTypeCode(Types.REAL);
                break;
            case 22:
                if (column.getScale() == 0) {
                    column.setTypeCode(Types.INTEGER);
                }
                break;
            case 38:
                if (column.getScale() == 0) {
                    column.setTypeCode(Types.BIGINT);
                } else {
                    column.setTypeCode(Types.DOUBLE);
                }
                break;
            }
        } else if (column.getTypeCode() == Types.FLOAT) {
            // Same for REAL, FLOAT, DOUBLE PRECISION, which all back-map to
            // FLOAT but with
            // different sizes (63 for REAL, 126 for FLOAT/DOUBLE PRECISION)
            switch (column.getSizeAsInt()) {
            case 63:
                column.setTypeCode(Types.REAL);
                break;
            case 126:
                column.setTypeCode(Types.DOUBLE);
                break;
            }
        } else if ((column.getTypeCode() == Types.DATE)
                || (column.getTypeCode() == Types.TIMESTAMP)) {
            // Oracle has only one DATE/TIME type, so we can't know which it is
            // and thus map
            // it back to TIMESTAMP
            column.setTypeCode(Types.TIMESTAMP);

            // we also reverse the ISO-format adaptation, and adjust the default
            // value to timestamp
            if (column.getDefaultValue() != null) {
                Timestamp timestamp = null;

                Matcher matcher = oracleIsoTimestampPattern.matcher(column.getDefaultValue());
                if (matcher.matches()) {
                    String timestampVal = matcher.group(1);
                    timestamp = Timestamp.valueOf(timestampVal);
                } else {
                    matcher = oracleIsoDatePattern.matcher(column.getDefaultValue());
                    if (matcher.matches()) {
                        String dateVal = matcher.group(1);
                        timestamp = new Timestamp(Date.valueOf(dateVal).getTime());
                    } else {
                        matcher = oracleIsoTimePattern.matcher(column.getDefaultValue());
                        if (matcher.matches()) {
                            String dateVal = matcher.group(1);
                            timestamp = new Timestamp(Time.valueOf(dateVal).getTime());
                        }
                    }
                }

                if (timestamp != null) {
                    column.setDefaultValue(timestamp.toString());
                }
            }
        } else if (TypeMap.isTextType(column.getTypeCode())) {
            column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
        }
        return column;
    }

    /**
     * Helper method that determines the auto increment status using Firebird's
     * system tables.
     * 
     * @param table
     *            The table
     */
    protected void determineAutoIncrementColumns(Connection c, Table table) throws SQLException {
        Column[] columns = table.getColumns();

        for (int idx = 0; idx < columns.length; idx++) {
            columns[idx].setAutoIncrement(isAutoIncrement(c, table, columns[idx]));
        }
    }

    /**
     * Tries to determine whether the given column is an identity column.
     * 
     * @param table
     *            The table
     * @param column
     *            The column
     * @return <code>true</code> if the column is an identity column
     */
    protected boolean isAutoIncrement(Connection c, Table table, Column column) throws SQLException {
        // TODO: For now, we only check whether there is a sequence & trigger as
        // generated by DdlUtils
        // But once sequence/trigger support is in place, it might be possible
        // to 'parse' the
        // trigger body (via SELECT trigger_name, trigger_body FROM
        // user_triggers) in order to
        // determine whether it fits our auto-increment definition
        PreparedStatement prepStmt = null;
        ResultSet resultSet = null;
        String triggerName = getPlatform().getConstraintName("trg", table, column.getName(), null);
        String seqName = getPlatform().getConstraintName("seq", table, column.getName(), null);

        if (!getPlatform().getPlatformInfo().isDelimitedIdentifierModeOn()) {
            triggerName = triggerName.toUpperCase();
            seqName = seqName.toUpperCase();
        }

        try {
            prepStmt = c.prepareStatement("SELECT * FROM user_triggers WHERE trigger_name = ?");
            prepStmt.setString(1, triggerName);

            resultSet = prepStmt.executeQuery();

            if (!resultSet.next()) {
                return false;
            }
            // we have a trigger, so lets check the sequence
            JdbcSqlConnection.close(resultSet);
            JdbcSqlConnection.close(prepStmt);

            prepStmt = c.prepareStatement("SELECT * FROM user_sequences WHERE sequence_name = ?");
            prepStmt.setString(1, seqName);

            resultSet = prepStmt.executeQuery();
            return resultSet.next();
        } finally {
            JdbcSqlConnection.close(resultSet);
            JdbcSqlConnection.close(prepStmt);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected Collection<Index> readIndices(Connection c, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        // Oracle bug 4999817 causes a table analyze to execute in response to a
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

        StringBuffer query = new StringBuffer();

        query.append("SELECT a.INDEX_NAME, a.INDEX_TYPE, a.UNIQUENESS, b.COLUMN_NAME, b.COLUMN_POSITION FROM USER_INDEXES a, USER_IND_COLUMNS b WHERE ");
        query.append("a.TABLE_NAME=? AND a.GENERATED=? AND a.TABLE_TYPE=? AND a.TABLE_NAME=b.TABLE_NAME AND a.INDEX_NAME=b.INDEX_NAME AND ");
        query.append("a.INDEX_NAME NOT IN (SELECT DISTINCT c.CONSTRAINT_NAME FROM USER_CONSTRAINTS c WHERE c.CONSTRAINT_TYPE=? AND c.TABLE_NAME=a.TABLE_NAME");
        if (metaData.getSchemaPattern() != null) {
            query.append(" AND c.OWNER LIKE ?) AND a.TABLE_OWNER LIKE ?");
        } else {
            query.append(")");
        }

        Map<String, Index> indices = new LinkedHashMap<String, Index>();
        PreparedStatement stmt = null;

        try {
            stmt = c.prepareStatement(query.toString());
            stmt.setString(1,
                    getPlatform().getPlatformInfo().isDelimitedIdentifierModeOn() ? tableName
                            : tableName.toUpperCase());
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
                // Only read in normal oracle indexes
                if (type.startsWith("NORMAL")) {
                    values.put("INDEX_TYPE", new Short(DatabaseMetaData.tableIndexOther));
                    values.put("INDEX_NAME", name);
                    values.put("NON_UNIQUE",
                            "UNIQUE".equalsIgnoreCase(rs.getString(3)) ? Boolean.FALSE
                                    : Boolean.TRUE);
                    values.put("COLUMN_NAME", rs.getString(4));
                    values.put("ORDINAL_POSITION", new Short(rs.getShort(5)));

                    readIndex(metaData, values, indices);
                } else {
                    log.log(LogLevel.WARN, "Skipping index %s of type %s", name, type);
                }
            }
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return indices.values();
    }

    @Override
    protected Table readTable(Connection c, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        // Oracle 10 added the recycle bin which contains dropped database
        // objects not yet purged
        // Since we don't want entries from the recycle bin, we filter them out
        PreparedStatement stmt = null;
        boolean deletedObj = false;

        try {
            stmt = c.prepareStatement("SELECT * FROM RECYCLEBIN WHERE OBJECT_NAME=?");
            stmt.setString(1, (String) values.get("TABLE_NAME"));

            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                // we found the table in the recycle bin, so its a deleted one
                // which we ignore
                deletedObj = true;
            }
            rs.close();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

        if (!deletedObj) {
            String tableName = (String) values.get("TABLE_NAME");

            // system table ?
            if (tableName.indexOf('$') > 0) {
                return null;
            }

            Table table = super.readTable(c, metaData, values);

            if (table != null) {
                determineAutoIncrementColumns(c, table);
            }

            return table;

        } else {
            return null;
        }
    }

}
