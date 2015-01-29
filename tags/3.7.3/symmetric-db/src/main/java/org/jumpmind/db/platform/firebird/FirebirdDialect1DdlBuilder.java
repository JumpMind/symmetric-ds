package org.jumpmind.db.platform.firebird;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;

import org.jumpmind.db.alter.ColumnDataTypeChange;
import org.jumpmind.db.alter.ColumnDefaultValueChange;
import org.jumpmind.db.alter.TableChange;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.DatabaseNamesConstants;

/*
 * The SQL builder for Firebird running in SQL dialect 1 mode
 */
public class FirebirdDialect1DdlBuilder extends FirebirdDdlBuilder {

    public FirebirdDialect1DdlBuilder() {
        databaseInfo.addNativeTypeMapping(Types.BIGINT, "NUMERIC(18)", Types.NUMERIC);
        databaseInfo.addNativeTypeMapping(Types.TIME, "TIMESTAMP", Types.TIMESTAMP);
        databaseName = DatabaseNamesConstants.FIREBIRD_DIALECT1;
        databaseInfo.setDelimitedIdentifiersSupported(false);
        databaseInfo.setDelimiterToken("");
    }

    protected void processTableStructureChanges(Database currentModel, Database desiredModel,
            Collection<TableChange> changes, StringBuilder ddl) {
        Iterator<TableChange> iter = changes.iterator();
        while (iter.hasNext()) {
            TableChange change = iter.next();
            if (change instanceof ColumnDataTypeChange) {
                ColumnDataTypeChange dataTypeChange = (ColumnDataTypeChange) change;
                if (dataTypeChange.getNewTypeCode() == Types.BIGINT &&
                        dataTypeChange.getChangedColumn().getMappedTypeCode() == Types.DOUBLE) {
                    iter.remove();
                } 
            } else if (change instanceof ColumnDefaultValueChange) {
                ColumnDefaultValueChange defaultValueChange = (ColumnDefaultValueChange) change;
                if (defaultValueChange.getChangedColumn().getMappedTypeCode() == Types.DOUBLE &&
                        new BigDecimal(defaultValueChange.getNewDefaultValue()).equals(
                                new BigDecimal(defaultValueChange.getChangedColumn().getDefaultValue()))) {
                    iter.remove();
                }
            }
        }
        super.processTableStructureChanges(currentModel, desiredModel, changes, ddl);
    }
}
