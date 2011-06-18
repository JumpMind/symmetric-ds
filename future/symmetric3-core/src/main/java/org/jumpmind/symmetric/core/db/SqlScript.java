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
package org.jumpmind.symmetric.core.db;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.common.IoUtils;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;

/**
 * This class is for running SQL scripts against an {@link ISqlTemplate}.
 */
public class SqlScript {

    static final String COMMENT_CHARS_1 = "--";
    static final String COMMENT_CHARS_2 = "#";

    static final Log log = LogFactory.getLog(SqlScript.class);

    public final static String QUERY_ENDS = ";";

    private String delimiter = QUERY_ENDS;

    private List<String> statements;

    private IDbDialect platform;

    private int commitRate = 10000;

    private boolean failOnError = true;

    private Map<String, String> replacementTokens;

    private String fileName = "memory";

    private String lineDeliminator;

    public SqlScript(URL url, IDbDialect platform) {
        this(url, platform, true, QUERY_ENDS, null);
    }

    public SqlScript(URL url, IDbDialect platform, boolean failOnError) {
        this(url, platform, failOnError, QUERY_ENDS, null);
    }

    public SqlScript(URL url, IDbDialect platform, String delimiter) {
        this(url, platform, true, delimiter, null);
    }

    public SqlScript(URL url, IDbDialect platform, boolean failOnError, String delimiter,
            Map<String, String> replacementTokens) {
        try {
            fileName = url.getFile();
            fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
            log.log(LogLevel.INFO, "Loading sql from script %s", fileName);
            init(IoUtils.readLines(new InputStreamReader(url.openStream(), "UTF-8")), platform,
                    failOnError, delimiter, replacementTokens);
        } catch (IOException ex) {
            log.error(ex);
            throw new RuntimeException(ex);
        }
    }

    public SqlScript(String sqlScript, IDbDialect platform) {
        this(sqlScript, platform, true, QUERY_ENDS, null);
    }
    
    public SqlScript(String sqlScript, IDbDialect platform, boolean failOnError) {
        this(sqlScript, platform, failOnError, QUERY_ENDS, null);
    }

    public SqlScript(String sqlScript, IDbDialect platform, boolean failOnError, String delimiter,
            Map<String, String> replacementTokens) {
        init(IoUtils.readLines(new StringReader(sqlScript)), platform, failOnError, delimiter,
                replacementTokens);
    }

    private void init(List<String> sqlScript, IDbDialect platform, boolean failOnError,
            String delimiter, Map<String, String> replacementTokens) {
        this.statements = parseLines(sqlScript);
        this.platform = platform;
        this.failOnError = failOnError;
        this.delimiter = delimiter;
        this.replacementTokens = replacementTokens;
    }

    public int execute() {
        return execute(false);
    }

    protected List<String> parseLines(List<String> script) {
        ArrayList<String> statements = new ArrayList<String>();
        int lineCount = 0;

        StringBuilder sql = new StringBuilder();
        int count = 0;
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
                    toExecute = StringUtils.replaceTokens(toExecute, replacementTokens, false);
                    if (StringUtils.isNotBlank(toExecute)) {
                        statements.add(toExecute);
                        count++;
                    }

                    sql.setLength(0);
                } else {
                    sql.append("\n");
                    sql.append(line);
                }
            }
        }
        return statements;
    }

    public int execute(final boolean autoCommit) {
        ISqlTemplate connection = platform.getSqlTemplate();
        return connection.update(autoCommit, failOnError, commitRate,
                statements.toArray(new String[statements.size()]));
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

}