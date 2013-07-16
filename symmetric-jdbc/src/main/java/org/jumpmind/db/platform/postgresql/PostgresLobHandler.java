package org.jumpmind.db.platform.postgresql;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jumpmind.db.sql.SymmetricLobHandler;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

public class PostgresLobHandler extends SymmetricLobHandler {

    private LobHandler wrappedLobHandler = null;

    public PostgresLobHandler() {
        super();
        DefaultLobHandler wrappedLobHandler = new DefaultLobHandler();
        wrappedLobHandler.setWrapAsLob(true);
        this.wrappedLobHandler = wrappedLobHandler;
    }

    public boolean needsAutoCommitFalseForBlob(int jdbcTypeCode, String jdbcTypeName) {
        return PostgreSqlDatabasePlatform.isBlobStoredByReference(jdbcTypeName);
    }

    public byte[] getBlobAsBytes(ResultSet rs, int columnIndex, int jdbcTypeCode, String jdbcTypeName)
            throws SQLException {

        if (PostgreSqlDatabasePlatform.isBlobStoredByReference(jdbcTypeName)) {
            return wrappedLobHandler.getBlobAsBytes(rs, columnIndex);
        } else {
            return getDefaultHandler().getBlobAsBytes(rs, columnIndex);
        }
    }
}
