package org.jumpmind.symmetric.ddlutils.informix;

import java.io.IOException;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.alteration.PrimaryKeyChange;
import org.apache.ddlutils.alteration.RemovePrimaryKeyChange;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.platform.SqlBuilder;

public class InformixBuilder extends SqlBuilder {

    public InformixBuilder(Platform platform) {
	super(platform);
    }

    @Override
    protected void writeColumn(Table table, Column column) throws IOException {
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
	    throws IOException {
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

    protected void writeExternalForeignKeyCreateStmt(Database database, Table table, ForeignKey key) throws IOException {
        if (key.getForeignTableName() == null) {
            _log.warn("Foreign key table is null for key " + key);
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
    
    protected void processChange(Database currentModel, Database desiredModel, RemovePrimaryKeyChange change)
	    throws IOException {
	print("ALTER TABLE ");
	printlnIdentifier(getTableName(change.getChangedTable()));
	printIndent();
	print("DROP CONSTRAINT ");
	printIdentifier(getConstraintName(null, change.getChangedTable(), "PK", null));
	printEndOfStatement();
	change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }

    protected void processChange(Database currentModel, Database desiredModel, PrimaryKeyChange change)
	    throws IOException {
	print("ALTER TABLE ");
	printlnIdentifier(getTableName(change.getChangedTable()));
	printIndent();
	print("DROP CONSTRAINT ");
	printIdentifier(getConstraintName(null, change.getChangedTable(), "PK", null));
	printEndOfStatement();
	writeExternalPrimaryKeysCreateStmt(change.getChangedTable(), change.getNewPrimaryKeyColumns());
	change.apply(currentModel, getPlatform().isDelimitedIdentifierModeOn());
    }
}
