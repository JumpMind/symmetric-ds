/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.StatementCallback;

/**
 * This class is for running sql scripts against a datasource.
 */
public class SqlScript {

    static final Log logger = LogFactory.getLog(SqlScript.class);

    public final static char QUERY_ENDS = ';';

    private char delimiter = QUERY_ENDS;

    private URL script;

    private DataSource dataSource;

    private boolean failOnError = true;

    public SqlScript(URL url, DataSource dataSource) {
        this.script = url;
        this.dataSource = dataSource;
    }

    public SqlScript(URL url, DataSource dataSource, boolean failOnError) {
        this(url, dataSource);
        this.failOnError = failOnError;
    }

    public SqlScript(URL url, DataSource dataSource, char delimiter) {
        this(url, dataSource);
        this.delimiter = delimiter;
    }

    private boolean isComment(String line) {
        if ((line != null) && (line.length() > 0))
            return (line.charAt(0) == '#' || line.startsWith("--"));
        return false;
    }

    public void execute() {
        JdbcTemplate template = new JdbcTemplate(dataSource);
        template.execute(new StatementCallback() {
            public Object doInStatement(Statement statement) throws SQLException, DataAccessException {
                executeScript(statement);
                return null;
            }
        });
    }

    private void executeScript(Statement st) throws SQLException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(script.openStream()));
            String line;
            StringBuilder sql = new StringBuilder();

            while ((line = reader.readLine()) != null) {
                if (! isComment(line)) {
                    if (checkStatementEnds(line)) {
                        sql.append(line.substring(0, line.indexOf(delimiter)));
                        if (logger.isDebugEnabled()) {
                            logger.debug("query->" + sql);
                        }
                        try {
                            st.execute(sql.toString());
                        } catch (SQLException e) {
                            if (failOnError) {
                                logger.error(sql.toString() + " failed to execute.", e);
                                throw e;
                            } else {
                                logger.warn(e.getMessage() + ": " + sql.toString());
                            }
                        }
                        sql.setLength(0);
                    } else {
                        sql.append(line);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean checkStatementEnds(String s) {
        return s.trim().endsWith("" + delimiter);
    }

}