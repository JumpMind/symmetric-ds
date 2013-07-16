package org.jumpmind.db.sql;

public class SqlToken {

    String replacementToken;
    Object value;

    public SqlToken(String replacementToken, Object value) {
        this.replacementToken = replacementToken;
        this.value = value;
    }

    public String getReplacementToken() {
        return replacementToken;
    }

    public Object getValue() {
        return value;
    }

}
