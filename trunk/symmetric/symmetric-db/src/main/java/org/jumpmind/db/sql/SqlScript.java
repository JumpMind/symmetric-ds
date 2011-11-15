/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */
package org.jumpmind.db.sql;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.jdbc.IConnectionCallback;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.jumpmind.util.FormatUtils;

/**
 * This class is for running SQL scripts against a DataSource.
 */
public class SqlScript {

    static final String COMMENT_CHARS_1 = "--";
    static final String COMMENT_CHARS_2 = "#";

    public final static String QUERY_ENDS = ";";

    private String delimiter = QUERY_ENDS;

    private List<String> script;

    private DataSource dataSource;

    private int commitRate = 10000;

    private boolean failOnError = true;

    private Map<String, String> replacementTokens;

    private final static String MEMORY = "SQL snippet";

    private String fileName = MEMORY;

    private String lineDeliminator;

    private ISqlScriptListener listener;

    public SqlScript(URL url, DataSource ds) {
        this(url, ds, true, QUERY_ENDS, null);
    }

    public SqlScript(URL url, DataSource ds, boolean failOnError) {
        this(url, ds, failOnError, QUERY_ENDS, null);
    }

    public SqlScript(URL url, DataSource ds, String delimiter) {
        this(url, ds, true, delimiter, null);
    }

    public SqlScript(URL url, DataSource ds, boolean failOnError, String delimiter,
            Map<String, String> replacementTokens) {
        try {
            fileName = url.getFile();
            fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
            init(IOUtils.readLines(new InputStreamReader(url.openStream(), "UTF-8")), ds,
                    failOnError, delimiter, replacementTokens);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SqlScript(String sqlScript, DataSource ds, boolean failOnError) {
        this(sqlScript, ds, failOnError, QUERY_ENDS, null);
    }

    public SqlScript(String sqlScript, DataSource ds, boolean failOnError, String delimiter,
            Map<String, String> replacementTokens) {
        try {
            init(IOUtils.readLines(new StringReader(sqlScript)), ds, failOnError, delimiter,
                    replacementTokens);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setListener(ISqlScriptListener listener) {
        this.listener = listener;
    }

    public ISqlScriptListener getListener() {
        return listener;
    }

    private void init(List<String> sqlScript, DataSource ds, boolean failOnError, String delimiter,
            Map<String, String> replacementTokens) {
        this.script = sqlScript;
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
            }
        }
    }

    private void closeQuietly(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
            }
        }
    }

    public long execute() {
        return execute(false);
    }

    public long execute(final boolean autoCommit) {
        JdbcSqlTemplate template = new JdbcSqlTemplate(dataSource);
        return template.execute(new IConnectionCallback<Long>() {
            public Long execute(Connection connection) throws SQLException {
                Statement st = null;
                ResultSet rs = null;
                int lineCount = 0;
                long totalRowsUpdatedCount = 0;
                long totalRowsRead = 0;
                int errorCount = 0;
                try {
                    connection.setAutoCommit(autoCommit);
                    st = connection.createStatement();
                    StringBuilder sql = new StringBuilder();
                    int count = 0;
                    int unableToDropCount = 0;
                    for (String line : script) {
                        lineCount++;
                        line = trimComments(line);
                        if (line.trim().length() > 0) {
                            if (checkStatementEnds(line)) {
                                if (sql.length() > 0) {
                                    sql.append("\n");
                                }
                                sql.append(line.substring(0, line.lastIndexOf(delimiter)).trim());
                                String toExecute = sql.toString();
                                if (StringUtils.isNotBlank(lineDeliminator)) {
                                    toExecute = toExecute.replaceAll(lineDeliminator, "\n");
                                }
                                try {
                                    toExecute = FormatUtils.replaceTokens(toExecute,
                                            replacementTokens, false);

                                    if (StringUtils.isNotBlank(toExecute)) {
                                        if (listener != null) {
                                            listener.sqlToRun(sql.toString());
                                        }
                                        long rowsRetrieved = 0;
                                        long rowsUpdated = 0;
                                        long ts = System.currentTimeMillis();
                                        if (st.execute(toExecute)) {
                                            rs = st.getResultSet();
                                            while (rs.next()) {
                                                rowsRetrieved++;
                                            }
                                            totalRowsRead += rowsRetrieved;
                                        } else {
                                            rowsUpdated = st.getUpdateCount();
                                            totalRowsUpdatedCount += rowsUpdated;
                                        }

                                        if (listener != null) {
                                            listener.sqlApplied(toExecute, rowsUpdated,
                                                    rowsRetrieved, lineCount,
                                                    System.currentTimeMillis() - ts);
                                        }

                                        count++;
                                        if (count % commitRate == 0) {
                                            connection.commit();
                                        }
                                    }
                                } catch (SQLException e) {
                                    errorCount++;
                                    if (listener != null) {
                                        listener.sqlErrored(toExecute, e, lineCount);
                                    }
                                    if (failOnError) {
                                        throw e;
                                    } else {
                                        if (sql.toString().toLowerCase().startsWith("drop")) {
                                            unableToDropCount++;
                                        }
                                    }
                                }
                                sql.setLength(0);
                            } else {
                                sql.append("\n");
                                sql.append(line);
                            }
                        }
                    }

                    if (!autoCommit) {
                        connection.commit();
                    }

                    if (listener != null) {
                        listener.scriptCompleted(lineCount, errorCount, totalRowsUpdatedCount,
                                totalRowsRead, unableToDropCount);
                    }

                } catch (Exception e) {
                    throw new SqlScriptException(e, lineCount);
                } finally {
                    closeQuietly(rs);
                    closeQuietly(st);
                }
                return totalRowsUpdatedCount;
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
        return line;
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

    public void setLineDeliminator(String lineDeliminator) {
        this.lineDeliminator = lineDeliminator;
    }

    public static interface ISqlScriptListener {

        public void sqlToRun(String sql);

        public void sqlApplied(String sql, long rowsUpdated, long rowsRetreived, int lineNumber,
                long timeToExecute);

        public void sqlErrored(String sql, SQLException ex, int lineNumber);

        public void scriptCompleted(int statementCount, int statementsThatErroredCount,
                long rowsAffectedCount, long rowsReadCount, int failedDropsCount);
    }

}