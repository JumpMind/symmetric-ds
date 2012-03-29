package org.jumpmind.symmetric.android.db;

import org.jumpmind.symmetric.core.db.AbstractTableBuilder;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.Index;
import org.jumpmind.symmetric.core.model.Table;

public class SQLiteTableBuilder extends AbstractTableBuilder {

    public SQLiteTableBuilder(IDbDialect platform) {
        super(platform);
    }

    public void writeExternalIndexDropStmt(Table table, Index index) {
        print("DROP INDEX ");
        printIdentifier(getIndexName(index));
        printEndOfStatement();
    }
    
}
