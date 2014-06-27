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
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * This class is for running SQL scripts against a DataSource.
 */
public class SqlScript {

    static final String COMMENT_CHARS_1 = "--";
    static final String COMMENT_CHARS_2 = "#";

    static final Log logger = LogFactory.getLog(SqlScript.class);

    public final static char QUERY_ENDS = ';';

    private char delimiter = QUERY_ENDS;

    private URL script;

    private DataSource dataSource;

    private int commitRate = 10000;

    private boolean failOnError = true;

    private Map<String, String> replacementTokens;

    public SqlScript(URL url, DataSource ds) {
        this(url, ds, true, QUERY_ENDS, null);
    }

    public SqlScript(URL url, DataSource ds, boolean failOnError) {
        this(url, ds, failOnError, QUERY_ENDS, null);
    }

    public SqlScript(URL url, DataSource ds, char delimiter) {
        this(url, ds, true, delimiter, null);
    }

    public SqlScript(URL url, DataSource ds, boolean failOnError, char delimiter, Map<String, String> replacementTokens) {
        this.script = url;
        this.dataSource = ds;
        this.failOnError = failOnError;
        this.delimiter = delimiter;
        this.replacementTokens = replacementTokens;
    }

    private void closeQuietly(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error(e, e);
            }
        }
    }

    protected String replaceTokens(String original) {
        if (this.replacementTokens != null) {
            for (Object key : this.replacementTokens.keySet()) {
                original = original.replaceAll("\\%" + key + "\\%", this.replacementTokens.get((String) key));
            }
        }
        return original;
    }

    public void execute() {
        JdbcTemplate template = new JdbcTemplate(this.dataSource);
        template.execute(new ConnectionCallback() {
            public Object doInConnection(Connection connection) throws SQLException, DataAccessException {
                Statement st = null;
                String fileName = script.getFile();
                fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
                logger.info("Running " + fileName);
                int lineCount = 0;

                try {
                    connection.setAutoCommit(false);
                    st = connection.createStatement();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(script.openStream()));
                    String line;
                    StringBuilder sql = new StringBuilder();
                    int count = 0;
                    int notFoundCount = 0;
                    while ((line = reader.readLine()) != null) {
                        lineCount++;
                        line = trimComments(line);
                        if (line.length() > 0) {
                            if (checkStatementEnds(line)) {
                                sql.append(" ");
                                sql.append(line.substring(0, line.lastIndexOf(delimiter)));
                                if (logger.isDebugEnabled()) {
                                    logger.debug("query->" + sql);
                                }
                                try {
                                    st.execute(replaceTokens(sql.toString()));
                                    count++;
                                    if (count % commitRate == 0) {
                                        connection.commit();
                                    }
                                } catch (SQLException e) {
                                    if (failOnError) {
                                        logger.error(sql.toString() + " failed to execute.", e);
                                        throw e;
                                    } else {
                                        if (e.getErrorCode() != 942 && e.getErrorCode() != 2289) {
                                            logger.warn(e.getMessage() + ": " + sql.toString());
                                        } else if (sql.toString().toLowerCase().startsWith("drop")) {
                                            notFoundCount++;
                                        }
                                    }
                                }
                                sql.setLength(0);
                            } else {
                                sql.append(" ");
                                sql.append(line);
                            }
                        }
                    }

                    connection.commit();

                    logger.info("Ran " + count + " sql statements in " + fileName);
                    if (notFoundCount > 0) {
                        logger.info("Could not drop a total of " + notFoundCount
                                + " database object because they were not found");
                    }
                } catch (Exception e) {
                    logger.info("Error on line " + lineCount + " of " + fileName);
                    throw new RuntimeException(e);
                } finally {
                    closeQuietly(st);
                }
                return null;
            }
        });
    }

    private String trimComments(String line) {
        int index = line.indexOf(COMMENT_CHARS_1);
        if (index >= 0) {
            line = line.substring(0, index);
        }
        index = line.indexOf(COMMENT_CHARS_2);
        if (index >= 0) {
            line = line.substring(0, index);
        }
        return line.trim();
    }

    private boolean checkStatementEnds(String s) {
        return s.trim().endsWith("" + delimiter);
    }

    public int getCommitRate() {
        return commitRate;
    }

    public void setCommitRate(int commitRate) {
        this.commitRate = commitRate;
    }
}