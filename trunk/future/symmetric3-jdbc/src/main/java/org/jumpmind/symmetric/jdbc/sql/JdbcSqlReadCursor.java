package org.jumpmind.symmetric.jdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.sql.ISqlReadCursor;
import org.jumpmind.symmetric.core.sql.ISqlRowMapper;
import org.jumpmind.symmetric.jdbc.db.IJdbcDbPlatform;

public class JdbcSqlReadCursor<T> implements ISqlReadCursor<T> {

    protected IJdbcDbPlatform dbPlatform;

    protected Connection c;

    protected ResultSet rs;

    protected PreparedStatement st;

    protected boolean autoCommitFlag;

    protected ISqlRowMapper<T> mapper;

    protected int rowNumber;

    public JdbcSqlReadCursor(String sql, Object[] values, int[] types, ISqlRowMapper<T> mapper,
            IJdbcDbPlatform dbPlatform) {
        this.mapper = mapper;
        this.dbPlatform = dbPlatform;
        Parameters parameters = dbPlatform.getParameters();
        try {
            c = dbPlatform.getDataSource().getConnection();
            autoCommitFlag = c.getAutoCommit();

            if (dbPlatform.getPlatformInfo().isRequiresAutoCommitFalseToSetFetchSize()) {
                c.setAutoCommit(false);
            }

            st = c.prepareStatement(sql, java.sql.ResultSet.TYPE_FORWARD_ONLY,
                    java.sql.ResultSet.CONCUR_READ_ONLY);
            st.setQueryTimeout(parameters.getQueryTimeout());
            if (values != null) {
                StatementCreatorUtil.setValues(st, values, types, dbPlatform.getLobHandler());
            }
            st.setFetchSize(parameters.getStreamingFetchSize());
            rs = st.executeQuery();
        } catch (SQLException ex) {
            throw dbPlatform.getJdbcSqlConnection().translate(sql, ex);
        }
    }

    public T next() {
        try {
            if (rs.next()) {
                Map<String, Object> row = JdbcSqlConnection.getMapForRow(rs);
                return mapper.mapRow(row);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            throw dbPlatform.getJdbcSqlConnection().translate(ex);
        }
    }

    public void close() {
        JdbcSqlConnection.close(rs);
        JdbcSqlConnection.close(st);
        JdbcSqlConnection.close(autoCommitFlag, c);

    }

}
