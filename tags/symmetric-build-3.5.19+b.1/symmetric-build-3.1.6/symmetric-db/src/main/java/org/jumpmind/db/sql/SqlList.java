package org.jumpmind.db.sql;

import java.util.ArrayList;
import java.util.Collection;

public class SqlList extends ArrayList<Object> {

    private static final long serialVersionUID = 1L;

    protected String replacementToken;
    
    public SqlList(String replacementToken, Collection<?> originalList) {
        super(originalList);
        this.replacementToken = replacementToken;
    }

    public SqlList(String replacementToken, int size) {
        super(size);
        this.replacementToken = replacementToken;
    }

    public String getReplacementToken() {
        return replacementToken;
    }

}
