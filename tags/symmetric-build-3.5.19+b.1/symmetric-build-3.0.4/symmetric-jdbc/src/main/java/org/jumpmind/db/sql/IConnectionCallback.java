package org.jumpmind.db.sql;

import java.sql.Connection;
import java.sql.SQLException;

public interface IConnectionCallback<T> {

    public T execute(Connection con) throws SQLException;

}
