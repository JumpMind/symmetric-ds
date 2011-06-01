package org.jumpmind.symmetric.jdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.sql.ISqlReadCursor;
import org.jumpmind.symmetric.core.sql.ISqlRowMapper;
import org.jumpmind.symmetric.jdbc.db.IJdbcDbDialect;

public class JdbcSqlReadCursor<T> implements ISqlReadCursor<T> {

    protected IJdbcDbDialect dbDialect;

    protected Connection c;

    protected ResultSet rs;

    protected PreparedStatement st;

    protected boolean autoCommitFlag;

    protected ISqlRowMapper<T> mapper;

    protected int rowNumber;

    public JdbcSqlReadCursor(String sql, Object[] values, int[] types, ISqlRowMapper<T> mapper,
            IJdbcDbDialect dbPlatform) {
        this.mapper = mapper;
        this.dbDialect = dbPlatform;
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
                Map<String, Object> row = JdbcSqlTemplate.getMapForRow(rs);
                return mapper.mapRow(row);
            } else {
                return null;
            }
        } catch (SQLException ex) {
            throw dbDialect.getJdbcSqlConnection().translate(ex);
        }
    }

    public void close() {
        JdbcSqlTemplate.close(rs);
        JdbcSqlTemplate.close(st);
        JdbcSqlTemplate.close(autoCommitFlag, c);

    }

}
