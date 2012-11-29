package org.jumpmind.db.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

public class SymmetricLobHandler {

    private LobHandler lobHandler;

    public SymmetricLobHandler() {
        super();
        this.lobHandler = new DefaultLobHandler();
    }

    public SymmetricLobHandler(LobHandler lobHandler) {
        super();
        this.lobHandler = lobHandler;
    }

    public String getClobAsString(ResultSet rs, int columnIndex) throws SQLException {
        return lobHandler.getClobAsString(rs, columnIndex);
    }

    public byte[] getBlobAsBytes(ResultSet rs, int columnIndex, int jdbcTypeCode, String jdbcTypeName)
            throws SQLException {
        return lobHandler.getBlobAsBytes(rs, columnIndex);
    }

    public LobHandler getDefaultHandler() {
        return lobHandler;
    }

    public boolean needsAutoCommitFalseForBlob(int jdbcTypeCode, String jdbcTypeName) {
        return false;
    }

}
