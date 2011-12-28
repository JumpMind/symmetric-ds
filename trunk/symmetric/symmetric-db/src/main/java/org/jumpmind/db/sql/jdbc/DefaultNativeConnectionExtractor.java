package org.jumpmind.db.sql.jdbc;

import java.sql.Connection;

import org.apache.commons.dbcp.DelegatingConnection;

public class DefaultNativeConnectionExtractor implements INativeConnectionExtractor {

    public Connection extract(Connection c) {
        if (c instanceof DelegatingConnection) {
            return ((DelegatingConnection) c).getInnermostDelegate();
        } else {
            return c;
        }
    }

}
