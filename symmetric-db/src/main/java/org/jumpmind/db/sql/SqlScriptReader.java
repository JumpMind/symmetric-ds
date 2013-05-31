package org.jumpmind.db.sql;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.exception.IoException;
import org.jumpmind.util.FormatUtils;

/**
 * Provides an interface to read each SQL statement in a SQL script.
 */
public class SqlScriptReader extends LineNumberReader implements ISqlStatementSource {

    static final char COMMENT_CHARS[] = { '-', '#', '/' };

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
        int inLiteralStart = -1;
        int inLiteralEnd = -1;
        char[] content = line.toCharArray();
        for (int i = 0; i < line.length(); i++) {
            if (inLiteralStart == -1 && content[i] == '\'') {
                inLiteralStart = i;
                for (int j = inLiteralStart + 1; j < line.length(); j++) {
                    if (content[j] == '\'') {
                        if (j + 1 < content.length && content[j + 1] == '\'') {
                            j++;
                        } else {
                            inLiteralEnd = j + 1;
                            break;
                        }
                    }
                }
            }

            if (inLiteralEnd == i) {
                inLiteralEnd = -1;
                inLiteralStart = -1;
            }

            if (inLiteralStart == -1) {
                for (char c : COMMENT_CHARS) {
                    if (c == content[i]) {
                        if (i + 1 < content.length && content[i + 1] == c) {
                            return line.substring(0, i);
                        }

                        if (i > 0 && content[i - 1] == c) {
                            return line.substring(0, i - 1);
                        }
                    }
                }
            }
        }
        return line;
    }

    protected boolean checkStatementEnds(String s) {
        return s.trim().endsWith("" + delimiter);
    }

}
