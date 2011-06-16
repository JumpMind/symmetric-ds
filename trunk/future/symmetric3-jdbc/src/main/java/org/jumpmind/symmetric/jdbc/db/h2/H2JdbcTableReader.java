package org.jumpmind.symmetric.jdbc.db.h2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.ForeignKey;
import org.jumpmind.symmetric.core.model.Index;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.jdbc.db.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.jdbc.db.IJdbcDbDialect;
import org.jumpmind.symmetric.jdbc.db.JdbcTableReader;
import org.jumpmind.symmetric.jdbc.db.MetaDataColumnDescriptor;

public class H2JdbcTableReader extends JdbcTableReader {

    public H2JdbcTableReader(IJdbcDbDialect platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        if (values.get("CHARACTER_MAXIMUM_LENGTH") != null) {
            column.setSize(values.get("CHARACTER_MAXIMUM_LENGTH").toString());
        }
        if (values.get("COLUMN_DEFAULT") != null) {
            column.setDefaultValue(values.get("COLUMN_DEFAULT").toString());
        }
        if (values.get("NUMERIC_SCALE") != null) {
            column.setScale((Integer) values.get("NUMERIC_SCALE"));
        }
        
        if (TypeMap.isTextType(column.getTypeCode()) && (column.getDefaultValue() != null)) {
            column.setDefaultValue(StringUtils.strip(column.getDefaultValue(), "'"));
        }
        return column;
    }

    @Override
    protected List<MetaDataColumnDescriptor> initColumnsForColumn() {
        List<MetaDataColumnDescriptor> result = new ArrayList<MetaDataColumnDescriptor>();
        result.add(new MetaDataColumnDescriptor("COLUMN_DEF", 12));
        result.add(new MetaDataColumnDescriptor("COLUMN_DEFAULT", 12));
        result.add(new MetaDataColumnDescriptor("TABLE_NAME", 12));
        result.add(new MetaDataColumnDescriptor("COLUMN_NAME", 12));
        result.add(new MetaDataColumnDescriptor("DATA_TYPE", 4, new Integer(1111)));
        result.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", 4, new Integer(10)));
        result.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", 4, new Integer(0)));
        result.add(new MetaDataColumnDescriptor("NUMERIC_SCALE", 4, new Integer(0)));
        result.add(new MetaDataColumnDescriptor("COLUMN_SIZE", 12));
        result.add(new MetaDataColumnDescriptor("CHARACTER_MAXIMUM_LENGTH", 12));
        result.add(new MetaDataColumnDescriptor("IS_NULLABLE", 12, "YES"));
        result.add(new MetaDataColumnDescriptor("REMARKS", 12));
        return result;
    }

    @Override
    protected boolean isInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table,
            ForeignKey fk, Index index) {
        String name = index.getName();
        return name != null && name.startsWith("CONSTRAINT_INDEX_");
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table,
            Index index) {
        String name = index.getName();
        return name != null && name.startsWith("PRIMARY_KEY_");
    }

    @Override
    protected Table readTable(Connection c, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        Table table = super.readTable(c, metaData, values);

        if (table != null) {
            // H2 does not return the auto increment status in the meta data
            determineAutoIncrementFromResultSetMetaData(c, table, table.getPrimaryKeyColumnsArray());
        }

        return table;
    }

}
