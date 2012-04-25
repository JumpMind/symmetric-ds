package org.jumpmind.db.platform.greenplum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.postgresql.PostgreSqlJdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcUtils;
import org.springframework.jdbc.support.lob.LobHandler;

public class GreenplumJdbcSqlTemplate extends PostgreSqlJdbcSqlTemplate {

    public GreenplumJdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        super(dataSource, settings, lobHandler);
    }

    @Override
    public boolean supportsGetGeneratedKeys() {
        return false;
    }

    @Override
    protected long insertWithGeneratedKey(Connection conn, String sql, String column,
            String sequenceName, Object[] args, int[] types) throws SQLException {
        long key = 0;
        PreparedStatement ps = null;
        try {
            Statement st = null;
            ResultSet rs = null;
            try {
                st = conn.createStatement();
                rs = st.executeQuery("select nextval('" + sequenceName + "_seq')");
                if (rs.next()) {
                    key = rs.getLong(1);
                }
            } finally {
                close(rs);
                close(st);
            }

            String replaceSql = sql.replaceFirst("\\(null,", "(" + key + ",");
            ps = conn.prepareStatement(replaceSql);
            ps.setQueryTimeout(settings.getQueryTimeout());
            JdbcUtils.setValues(ps, args, types, lobHandler);
            ps.executeUpdate();
        } finally {
            close(ps);
        }
        return key;
    }

}
