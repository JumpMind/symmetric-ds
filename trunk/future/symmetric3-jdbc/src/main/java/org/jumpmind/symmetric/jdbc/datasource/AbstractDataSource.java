/*
 * Copyright 2002-2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jumpmind.symmetric.jdbc.datasource;

import java.io.PrintWriter;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Abstract base class for Spring's {@link javax.sql.DataSource}
 * implementations, taking care of the padding.
 * 
 * <p>
 * 'Padding' in the context of this class means default implementations for
 * certain methods from the <code>DataSource</code> interface, such as
 * {@link #getLoginTimeout()}, {@link #setLoginTimeout(int)}, and so forth.
 * 
 * @author Juergen Hoeller
 * @since 07.05.2003
 * @see DriverManagerDataSource
 */
public abstract class AbstractDataSource implements DataSource {

    /**
     * Returns 0, indicating the default system timeout is to be used.
     */
    public int getLoginTimeout() throws SQLException {
        return 0;
    }

    /**
     * Setting a login timeout is not supported.
     */
    public void setLoginTimeout(int timeout) throws SQLException {
        throw new UnsupportedOperationException("setLoginTimeout");
    }

    /**
     * LogWriter methods are not supported.
     */
    public PrintWriter getLogWriter() {
        throw new UnsupportedOperationException("getLogWriter");
    }

    /**
     * LogWriter methods are not supported.
     */
    public void setLogWriter(PrintWriter pw) throws SQLException {
        throw new UnsupportedOperationException("setLogWriter");
    }

    // ---------------------------------------------------------------------
    // Implementation of JDBC 4.0's Wrapper interface
    // ---------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (!DataSource.class.equals(iface)) {
            throw new SQLException("DataSource of type [" + getClass().getName()
                    + "] can only be unwrapped as [javax.sql.DataSource], not as ["
                    + iface.getName());
        }
        return (T) this;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return DataSource.class.equals(iface);
    }

}
