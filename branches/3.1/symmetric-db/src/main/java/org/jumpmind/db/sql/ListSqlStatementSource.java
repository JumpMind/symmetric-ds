package org.jumpmind.db.sql;

import java.util.ArrayList;
import java.util.List;

public class ListSqlStatementSource implements ISqlStatementSource {

    protected List<String> statements;
    
    public ListSqlStatementSource(String... statements) {
        this.statements = new ArrayList<String>();
        for (String sql : statements) {
            this.statements.add(sql);
        }
    }

    public ListSqlStatementSource(List<String> statements) {
        this.statements = new ArrayList<String>(statements);
    }

    public String readSqlStatement() {
        return statements.size() > 0 ? statements.remove(0) : null;
    }

}
