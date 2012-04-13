package org.jumpmind.db.platform.informix;

import org.jumpmind.db.alter.PrimaryKeyChange;
import org.jumpmind.db.alter.RemovePrimaryKeyChange;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractDdlBuilder;
import org.jumpmind.db.platform.DatabasePlatformInfo;

public class InformixDdlBuilder extends AbstractDdlBuilder {

    public InformixDdlBuilder(DatabasePlatformInfo platformInfo) {
        super(platformInfo);
    }

    @Override
    protected void writeColumn(Table table, Column column, StringBuilder ddl) {
        if (column.isAutoIncrement()) {
            printIdentifier(getColumnName(column), ddl);
            ddl.append(" SERIAL");
        } else {
            super.writeColumn(table, column, ddl);
        }
    }

    @Override
    public String getSelectLastIdentityValues(Table table) {
        return "select dbinfo('sqlca.sqlerrd1') from sysmaster:sysdual";
    }

    @Override
    protected void writeExternalPrimaryKeysCreateStmt(Table table, Column primaryKeyColumns[],
            StringBuilder ddl) {
        if (primaryKeyColumns.length > 0 && shouldGeneratePrimaryKeys(primaryKeyColumns)) {
            ddl.append("ALTER TABLE ");
            printlnIdentifier(getTableName(table.getName()), ddl);
            printIndent(ddl);
            ddl.append("ADD CONSTRAINT ");
            writePrimaryKeyStmt(table, primaryKeyColumns, ddl);
            ddl.append(" CONSTRAINT ");
            printIdentifier(getConstraintName(null, table, "PK", null), ddl);
            printEndOfStatement(ddl);
        }
    }

    protected void writeExternalForeignKeyCreateStmt(Database database, Table table,
            ForeignKey key, StringBuilder ddl) {
        if (key.getForeignTableName() == null) {
            log.warn("Foreign key table is null for key " + key);
        } else {
            writeTableAlterStmt(table, ddl);
            ddl.append("ADD CONSTRAINT FOREIGN KEY (");
            writeLocalReferences(key, ddl);
            ddl.append(") REFERENCES ");
            printIdentifier(getTableName(key.getForeignTableName()), ddl);
            ddl.append(" (");
            writeForeignReferences(key, ddl);
            ddl.append(") CONSTRAINT ");
            printIdentifier(getForeignKeyName(table, key), ddl);
            printEndOfStatement(ddl);
        }
    }

    protected void processChange(Database currentModel, Database desiredModel,
            RemovePrimaryKeyChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP CONSTRAINT ");
        printIdentifier(getConstraintName(null, change.getChangedTable(), "PK", null), ddl);
        printEndOfStatement(ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }

    protected void processChange(Database currentModel, Database desiredModel,
            PrimaryKeyChange change, StringBuilder ddl) {
        ddl.append("ALTER TABLE ");
        printlnIdentifier(getTableName(change.getChangedTable().getName()), ddl);
        printIndent(ddl);
        ddl.append("DROP CONSTRAINT ");
        printIdentifier(getConstraintName(null, change.getChangedTable(), "PK", null), ddl);
        printEndOfStatement(ddl);
        writeExternalPrimaryKeysCreateStmt(change.getChangedTable(),
                change.getNewPrimaryKeyColumns(), ddl);
        change.apply(currentModel, delimitedIdentifierModeOn);
    }
}
