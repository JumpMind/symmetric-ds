package org.jumpmind.symmetric.jdbc.db.oracle;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.jumpmind.symmetric.core.common.NotImplementedException;
import org.jumpmind.symmetric.jdbc.db.ILobHandler;

public class OracleLobHandler implements ILobHandler {

    public void setBlobAsBytes(PreparedStatement ps, int paramIndex, byte[] content)
            throws SQLException {
        throw new NotImplementedException();

    }

    public void setClobAsString(PreparedStatement ps, int paramIndex, String content)
            throws SQLException {
        throw new NotImplementedException();

    }

}
