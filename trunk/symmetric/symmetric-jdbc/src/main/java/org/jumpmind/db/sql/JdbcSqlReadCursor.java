package org.jumpmind.db.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcSqlReadCursor<T> implements ISqlReadCursor<T> {

    protected Connection c;

    protected ResultSet rs;

    protected PreparedStatement st;

    protected boolean autoCommitFlag;

    protected ISqlRowMapper<T> mapper;

    protected JdbcSqlTemplate sqlTemplate;

    protected int rowNumber;

    public JdbcSqlReadCursor() {
    }

    public JdbcSqlReadCursor(JdbcSqlTemplate sqlTemplate, ISqlRowMapper<T> mapper, String sql,
            Object[] values, int[] types) {
        this.sqlTemplate = sqlTemplate;
        this.mapper = mapper;
        try {
            c = sqlTemplate.getDataSource().getConnection();
            autoCommitFlag = c.getAutoCommit();

            if (sqlTemplate.isRequiresAutoCommitFalseToSetFetchSize()) {
                c.setAutoCommit(false);
            }

            st = c.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            st.setQueryTimeout(sqlTemplate.getSettings().getQueryTimeout());
            if (values != null) {
                JdbcUtils.setValues(st, values, types, sqlTemplate.getLobHandler());
            }
            st.setFetchSize(sqlTemplate.getSettings().getFetchSize());
            rs = st.executeQuery();
            SqlUtils.addSqlReadCursor(this);
        } catch (SQLException ex) {
            close();
            throw sqlTemplate.translate(sql, ex);
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
            throw sqlTemplate.translate(ex);
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
        SqlUtils.removeSqlReadCursor(this);
    }

}
