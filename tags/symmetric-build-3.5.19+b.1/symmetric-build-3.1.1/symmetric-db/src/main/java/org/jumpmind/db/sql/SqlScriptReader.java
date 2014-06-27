package org.jumpmind.db.sql;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.util.FormatUtils;

public class SqlScriptReader extends LineNumberReader {

    static final String COMMENT_CHARS[] = { "--", "#", "//" };

    public final static String QUERY_ENDS = ";";

    private String delimiter = QUERY_ENDS;

    private Map<String, String> replacementTokens;

    public SqlScriptReader(Reader in) {
        super(in);
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public void setReplacementTokens(Map<String, String> replacementTokens) {
        this.replacementTokens = replacementTokens;
    }

    public Map<String, String> getReplacementTokens() {
        return replacementTokens;
    }

    public String readSqlStatement() {
        try {
            String line = readLine();
            StringBuilder sql = null;
            if (line != null) {
                do {
                    line = trimComments(line);
                    if (StringUtils.isNotBlank(line)) {
                        if (sql == null) {
                            sql = new StringBuilder();
                        }
                        if (checkStatementEnds(line)) {
                            if (sql.length() > 0) {
                                sql.append("\n");
                            }
                            sql.append(line.substring(0, line.lastIndexOf(delimiter)).trim());
                            String toExecute = sql.toString();
                            toExecute = FormatUtils.replaceTokens(toExecute, replacementTokens,
                                    false);
                            if (StringUtils.isNotBlank(toExecute)) {
                                return toExecute.trim();
                            }
                        } else {
                            sql.append("\n");
                            sql.append(line);
                        }
                    }
                    line = readLine();
                } while (line != null);
                
                if (sql != null) {
                    return sql.toString();
                } else {
                    return null;
                }
            } else {
                return null;
            }

        } catch (IOException ex) {
            throw new IoException(ex);
        }

    }

    protected String trimComments(String line) {
        for (String commmentChar : COMMENT_CHARS) {
            if (line.startsWith(commmentChar)) {
                return null;
            }
        }
        return line;
    }

    protected boolean checkStatementEnds(String s) {
        return s.trim().endsWith("" + delimiter);
    }

}
