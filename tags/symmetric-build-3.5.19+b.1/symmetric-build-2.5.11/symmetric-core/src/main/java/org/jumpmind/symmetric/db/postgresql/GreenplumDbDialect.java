package org.jumpmind.symmetric.db.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.support.JdbcUtils;

public class GreenplumDbDialect extends PostgreSqlDbDialect {

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    public long insertWithGeneratedKey(final JdbcTemplate jdbcTemplate, final String sql,
            final SequenceIdentifier sequenceId, final PreparedStatementCallback<Object> callback) {
        return jdbcTemplate.execute(new ConnectionCallback<Long>() {
            public Long doInConnection(Connection conn) throws SQLException, DataAccessException {
                long key = 0;
                PreparedStatement ps = null;
                try {
                    Statement st = null;
                    ResultSet rs = null;
                    try {
                        st = conn.createStatement();
                        rs = st.executeQuery("select nextval('" + getSequenceName(sequenceId)
                                + "_seq')");
                        if (rs.next()) {
                            key = rs.getLong(1);
                        }
                    } finally {
                        JdbcUtils.closeResultSet(rs);
                        JdbcUtils.closeStatement(st);
                    }

                    String replaceSql = sql.replaceFirst("\\(null,", "(" + key + ",");
                    ps = conn.prepareStatement(replaceSql);
                    ps.setQueryTimeout(jdbcTemplate.getQueryTimeout());
                    if (callback != null) {
                        callback.doInPreparedStatement(ps);
                    }
                    ps.executeUpdate();
                } finally {
                    JdbcUtils.closeStatement(ps);
                }
                return key;
            }
        });
    }

}
