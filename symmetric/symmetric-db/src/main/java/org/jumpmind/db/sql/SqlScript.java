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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.util.FormatUtils;

/**
 * This class parses and runs SQL from an input file or buffer using the
 * designed {@link ISqlTemplate}.
 */
public class SqlScript {

    static final String COMMENT_CHARS_1 = "--";
    static final String COMMENT_CHARS_2 = "#";

    public final static String QUERY_ENDS = ";";

    private String delimiter = QUERY_ENDS;

    private List<String> statements;

    private ISqlTemplate sqlTemplate;

    private int commitRate = 10000;

    private boolean failOnError = true;

    private String lineDeliminator;

    private ISqlResultsListener resultsListener;

    public SqlScript(URL url, ISqlTemplate sqlTemplate) {
        this(url, sqlTemplate, true, QUERY_ENDS, null);
    }

    public SqlScript(URL url, ISqlTemplate sqlTemplate, boolean failOnError) {
        this(url, sqlTemplate, failOnError, QUERY_ENDS, null);
    }

    public SqlScript(URL url, ISqlTemplate sqlTemplate, String delimiter) {
        this(url, sqlTemplate, true, delimiter, null);
    }

    public SqlScript(URL url, ISqlTemplate sqlTemplate, boolean failOnError, String delimiter,
            Map<String, String> replacementTokens) {
        try {
            String fileName = url.getFile();
            fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
            init(IOUtils.readLines(new InputStreamReader(url.openStream(), "UTF-8")), sqlTemplate,
                    failOnError, delimiter, replacementTokens);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SqlScript(String sqlScript, ISqlTemplate sqlTemplate, boolean failOnError) {
        this(sqlScript, sqlTemplate, failOnError, QUERY_ENDS, null);
    }

    public SqlScript(String sqlScript, ISqlTemplate sqlTemplate, boolean failOnError,
            String delimiter, Map<String, String> replacementTokens) {
        try {
            init(IOUtils.readLines(new StringReader(sqlScript)), sqlTemplate, failOnError,
                    delimiter, replacementTokens);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void init(List<String> sqlScript, ISqlTemplate sqlTemplate, boolean failOnError,
            String delimiter, Map<String, String> replacementTokens) {
        this.statements = parseLines(sqlScript, replacementTokens);
        this.sqlTemplate = sqlTemplate;
        this.failOnError = failOnError;
        this.delimiter = delimiter;
    }

    protected List<String> parseLines(List<String> script, Map<String, String> replacementTokens) {
        List<String> statements = new ArrayList<String>();
        StringBuilder sql = new StringBuilder();
        for (String line : script) {
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
                    toExecute = FormatUtils.replaceTokens(toExecute, replacementTokens, false);
                    if (StringUtils.isNotBlank(toExecute)) {
                        statements.add(toExecute);
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

    public long execute() {
        return execute(false);
    }

    public long execute(final boolean autoCommit) {
        return sqlTemplate.update(autoCommit, failOnError, commitRate, resultsListener,
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

    public void setListener(ISqlResultsListener listener) {
        this.resultsListener = listener;
    }

}