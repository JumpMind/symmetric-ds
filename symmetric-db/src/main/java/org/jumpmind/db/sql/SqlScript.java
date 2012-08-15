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
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.Map;

import org.apache.commons.io.IOUtils;

/**
 * This class parses and runs SQL from an input file or buffer using the
 * designed {@link ISqlTemplate}.
 */
public class SqlScript {

    private ISqlTemplate sqlTemplate;

    private int commitRate = 10000;

    private boolean failOnError = true;

    private ISqlResultsListener resultsListener;

    private SqlScriptReader scriptReader;

    public SqlScript(URL url, ISqlTemplate sqlTemplate) {
        this(url, sqlTemplate, true, SqlScriptReader.QUERY_ENDS, null);
    }

    public SqlScript(URL url, ISqlTemplate sqlTemplate, boolean failOnError) {
        this(url, sqlTemplate, failOnError, SqlScriptReader.QUERY_ENDS, null);
    }

    public SqlScript(URL url, ISqlTemplate sqlTemplate, String delimiter) {
        this(url, sqlTemplate, true, delimiter, null);
    }

    public SqlScript(URL url, ISqlTemplate sqlTemplate, boolean failOnError, String delimiter,
            Map<String, String> replacementTokens) {
        try {
            String fileName = url.getFile();
            fileName = fileName.substring(fileName.lastIndexOf("/") + 1);
            init(new InputStreamReader(url.openStream(), "UTF-8"), sqlTemplate, failOnError,
                    delimiter, replacementTokens);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SqlScript(String sqlScript, ISqlTemplate sqlTemplate, boolean failOnError) {
        this(sqlScript, sqlTemplate, failOnError, SqlScriptReader.QUERY_ENDS, null);
    }

    public SqlScript(String sqlScript, ISqlTemplate sqlTemplate, boolean failOnError,
            String delimiter, Map<String, String> replacementTokens) {
        init(new StringReader(sqlScript), sqlTemplate, failOnError, delimiter, replacementTokens);
    }

    public SqlScript(Reader reader, ISqlTemplate sqlTemplate, boolean failOnError,
            String delimiter, Map<String, String> replacementTokens) {
        init(reader, sqlTemplate, failOnError, delimiter, replacementTokens);
    }

    private void init(Reader reader, ISqlTemplate sqlTemplate, boolean failOnError,
            String delimiter, Map<String, String> replacementTokens) {
        this.scriptReader = new SqlScriptReader(reader);
        this.scriptReader.setDelimiter(delimiter);
        this.scriptReader.setReplacementTokens(replacementTokens);
        this.sqlTemplate = sqlTemplate;
        this.failOnError = failOnError;
    }

    public long execute() {
        return execute(false);
    }

    public long execute(final boolean autoCommit) {
        try {
            long count = this.sqlTemplate.update(autoCommit, failOnError, commitRate,
                    this.resultsListener, this.scriptReader);
            return count;
        } finally {
            IOUtils.closeQuietly(this.scriptReader);
        }

    }

    public int getCommitRate() {
        return commitRate;
    }

    public void setCommitRate(int commitRate) {
        this.commitRate = commitRate;
    }

    public void setLineDeliminator(String lineDeliminator) {
        this.scriptReader.setDelimiter(lineDeliminator);
    }

    public void setListener(ISqlResultsListener listener) {
        this.resultsListener = listener;
    }

}