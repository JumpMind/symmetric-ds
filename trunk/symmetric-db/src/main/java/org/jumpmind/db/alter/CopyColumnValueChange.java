package org.jumpmind.db.alter;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;

public class CopyColumnValueChange extends TableChangeImplBase {
    
    private Column sourceColumn;
    
    private Column targetColumn;

    public CopyColumnValueChange(Table table, Column sourceColumn, Column targetColumn) {
        super(table);
        this.sourceColumn = sourceColumn;
        this.targetColumn = targetColumn;
    }

    public void apply(Database database, boolean caseSensitive) {
        // nothing to do.  structure hasn't changed.
    }
    
    public Column getSourceColumn() {
        return sourceColumn;
    }
    
    public Column getTargetColumn() {
        return targetColumn;
    }

}
