package org.jumpmind.symmetric.db;

import java.io.BufferedReader;
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
            public Object doInStatement(Statement stat) throws SQLException,
                    DataAccessException {
                try {
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(script.openStream()));
                    String line;
                    StringBuilder query = new StringBuilder();
                    boolean queryEnds = false;

                    while ((line = reader.readLine()) != null) {
                        if (isComment(line)) {
                            continue;
                        }
                        queryEnds = checkStatementEnds(line);

                        if (queryEnds) {
                            query.append(line.substring(0, line
                                    .indexOf(delimiter)));
                            if (logger.isDebugEnabled()) {
                                logger.debug("query->" + query);
                            }
                            stat.addBatch(query.toString());                            
                            if (!failOnError) {
                                try {
                                    stat.executeBatch();
                                } catch (SQLException ex) {
                                    logger.warn(ex.getMessage() + " for query -> " + query);
                                }
                            }
                            query.setLength(0);
                        } else {
                            query.append(line);
                        }
                    }

                    if (failOnError) {
                        stat.executeBatch();
                    }
                    return null;
                } catch (RuntimeException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        });
    }

    private boolean checkStatementEnds(String s) {
        return (s.indexOf(delimiter) != -1);
    }

}