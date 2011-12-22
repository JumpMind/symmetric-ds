package org.jumpmind.db.platform.sqlite;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.MetaDataColumnDescriptor;
import org.jumpmind.log.Log;

/*
 * Reads a database model from a SQLite database.
 */
public class SqLiteDdlReader extends AbstractJdbcDdlReader {

    public SqLiteDdlReader(Log log, IDatabasePlatform platform) {
        super(log, platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
    }

    protected Collection readForeignKeys(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        // TODO
        return new ArrayList();
    }

    protected Collection readIndices(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        // TODO
        return new ArrayList();
    }

    /* Below here copied from H2.  May still need tweaking */
    
    @Override
    @SuppressWarnings("unchecked")
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map values) throws SQLException {
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
            column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
        }
        return column;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected List initColumnsForColumn() {
        List result = new ArrayList();
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
    protected boolean isInternalForeignKeyIndex(Connection connection, DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk,
            IIndex index) {
        String name = index.getName();
        return name != null && name.startsWith("CONSTRAINT_INDEX_");
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection, DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        String name = index.getName();
        return name != null && name.startsWith("PRIMARY_KEY_");
    }
}
