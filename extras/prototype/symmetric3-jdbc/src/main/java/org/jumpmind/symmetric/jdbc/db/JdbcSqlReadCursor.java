package org.jumpmind.symmetric.jdbc.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.jumpmind.symmetric.core.db.ISqlReadCursor;
import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.model.Parameters;

public class JdbcSqlReadCursor<T> implements ISqlReadCursor<T> {

    protected IJdbcDbDialect dbDialect;

    protected Connection c;

    protected ResultSet rs;

    protected PreparedStatement st;

    protected boolean autoCommitFlag;

    protected ISqlRowMapper<T> mapper;

    protected int rowNumber;

    public JdbcSqlReadCursor(String sql, Object[] values, int[] types, ISqlRowMapper<T> mapper,
            IJdbcDbDialect dbDialect) {
        this.mapper = mapper;
        this.dbDialect = dbDialect;
        Parameters parameters = dbDialect.getParameters();
        try {
            c = dbDialect.getDataSource().getConnection();
            autoCommitFlag = c.getAutoCommit();

            if (dbDialect.getDbDialectInfo().isRequiresAutoCommitFalseToSetFetchSize()) {
                c.setAutoCommit(false);
            }

            st = c.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            st.setQueryTimeout(parameters.getQueryTimeout());
            if (values != null) {
                StatementCreatorUtil.setValues(st, values, types, dbDialect.getLobHandler());
            }
            st.setFetchSize(parameters.getStreamingFetchSize());
            rs = st.executeQuery();
        } catch (SQLException ex) {
            throw dbDialect.getJdbcSqlConnection().translate(sql, ex);
        }
    }

    public T next() {
        try {
            if (rs.next()) {
                Row row = getMapForRow();
                return mapper.mapRow(row);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            throw dbDialect.getJdbcSqlConnection().translate(ex);
        }
    }
    

    protected Row getMapForRow() throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        Row mapOfColValues = new Row(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            String key = JdbcSqlTemplate.lookupColumnName(rsmd, i);
            Object obj = JdbcSqlTemplate.getResultSetValue(rs, i);
            mapOfColValues.put(key, obj);
        }
        return mapOfColValues;
    }

    public void close() {
        JdbcSqlTemplate.close(rs);
        JdbcSqlTemplate.close(st);
        JdbcSqlTemplate.close(autoCommitFlag, c);

    }

}
