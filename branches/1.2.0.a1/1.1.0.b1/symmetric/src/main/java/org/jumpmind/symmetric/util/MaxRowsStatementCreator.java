package org.jumpmind.symmetric.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.jdbc.core.PreparedStatementCreator;

public class MaxRowsStatementCreator implements PreparedStatementCreator {

    private String sql;

    private int maxRows;

    public MaxRowsStatementCreator(String sql, int maxRows) {
        this.sql = sql;
        this.maxRows = maxRows;
    }

    public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement(sql);
        st.setMaxRows(maxRows);
        return st;
    }

}
