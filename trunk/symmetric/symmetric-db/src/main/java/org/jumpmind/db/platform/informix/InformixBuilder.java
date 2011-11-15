package org.jumpmind.db.platform.informix;

import java.io.Writer;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.SqlBuilder;
import org.jumpmind.util.Log;

public class InformixBuilder extends SqlBuilder {

    public InformixBuilder(Log log, IDatabasePlatform platform, Writer writer) {
        super(log, platform, writer);
    }

    @Override
    protected void writeColumn(Table table, Column column)  {
        if (column.isAutoIncrement()) {
            printIdentifier(getColumnName(column));
            print(" SERIAL");
        } else {
            super.writeColumn(table, column);
        }
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        return "select dbinfo('sqlca.sqlerrd1') from sysmaster:sysdual";
    }

    @Override
    protected void writeExternalPrimaryKeysCreateStmt(Table table, Column primaryKeyColumns[])
             {
        if (primaryKeyColumns.length > 0 && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
            print("ALTER TABLE ");
            printlnIdentifier(getTableName(table));
            printIndent();
            print("ADD CONSTRAINT ");
            writePrimaryKeyStmt(table, primaryKeyColumns);
            print(" CONSTRAINT ");
            printIdentifier(getConstraintName(null, table, "PK", null));
            printEndOfStatement();
        }
    }

    protected void writeExternalForeignKeyCreateStmt(Database database, Table table, ForeignKey key)
             {
        if (key.getForeignTableName() == null) {
            log.warn("Foreign key table is null for key " + key);
        } else {
            writeTableAlterStmt(table);
            print("ADD CONSTRAINT FOREIGN KEY (");
            writeLocalReferences(key);
            print(") REFERENCES ");
            printIdentifier(getTableName(database.findTable(key.getForeignTableName())));
            print(" (");
            writeForeignReferences(key);
            print(") CONSTRAINT ");
            printIdentifier(getForeignKeyName(table, key));
            printEndOfStatement();
        }
    }

    protected void processChange(Database currentModel, Database desiredModel,
            RemovePrimaryKeyChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP CONSTRAINT ");
        printIdentifier(getConstraintName(null, change.getChangedTable(), "PK", null));
        printEndOfStatement();
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }

    protected void processChange(Database currentModel, Database desiredModel,
            PrimaryKeyChange change)  {
        print("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable()));
        printIndent();
        print("DROP CONSTRAINT ");
        printIdentifier(getConstraintName(null, change.getChangedTable(), "PK", null));
        printEndOfStatement();
        writeExternalPrimaryKeysCreateStmt(change.getChangedTable(),
                change.getNewPrimaryKeyColumns());
        change.apply(currentModel, platform.isDelimitedIdentifierModeOn());
    }
}
