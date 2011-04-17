package org.jumpmind.symmetric.jdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.DbException;
import org.jumpmind.symmetric.core.db.DbIntegrityViolationException;
import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.jdbc.db.JdbcPlatformFactory;

public class Template {

    protected DataSource dataSource;
    protected IPlatform platform;

    public Template(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Template(IPlatform platform, DataSource dataSource) {
        this.dataSource = dataSource;
        this.platform = platform;
    }

    public IPlatform getPlatform() {
        if (this.platform == null) {
            this.platform = JdbcPlatformFactory.createPlatform(dataSource);
        }
        return this.platform;
    }

    public <T> T queryForObject(final String sql, Class<T> clazz, final Object... args) {
        return execute(new IConnectionCallback<T>() {
            @SuppressWarnings("unchecked")
            @Override
            public T execute(Connection con) throws SQLException {
                T result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    StatementCreatorUtil.setValues(ps, args);
                    rs = ps.executeQuery(sql);
                    if (rs.next()) {
                        result = (T) rs.getObject(1);
                    }
                } finally {
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }

    public void testConnection() {
        execute(new IConnectionCallback<Boolean>() {
            @Override
            public Boolean execute(Connection con) throws SQLException {
                return true;
            }
        });
    }

    public <T> T execute(IConnectionCallback<T> callback) {
        Connection c = null;
        try {
            c = dataSource.getConnection();
            return callback.execute(c);
        } catch (SQLException ex) {
            throw translate(ex);
        } finally {
            close(c);
        }
    }

    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (SQLException ex) {
        }
    }

    public DbException translate(SQLException ex) {
        if (getPlatform().isDataIntegrityException(ex)) {
            return new DbIntegrityViolationException(ex);
        } else {
            return new DbException(ex);
        }
    }

}
