package org.jumpmind.symmetric.android.db;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.db.AbstractDbDialect;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.db.SqlConstants;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Index;
import org.jumpmind.symmetric.core.model.IndexColumn;
import org.jumpmind.symmetric.core.model.NonUniqueIndex;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.core.model.UniqueIndex;

import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteOpenHelper;

public class SQLiteDbDialect extends AbstractDbDialect {

    final static ColumnMapper COLUMN_MAPPER = new ColumnMapper();
    final static IndexMapper INDEX_MAPPER = new IndexMapper();
    final static IndexColumnMapper INDEX_COLUMN_MAPPER = new IndexColumnMapper();

    protected SQLiteOpenHelper openHelper;

    protected SQLiteSqlTemplate sqlTemplate;

    public SQLiteDbDialect(SQLiteOpenHelper openHelper, Parameters parameters) {
        super(parameters);

        dialectInfo.setNonPKIdentityColumnsSupported(false);
        dialectInfo.setIdentityOverrideAllowed(false);
        dialectInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        dialectInfo.setNullAsDefaultValueRequired(false);

        dialectInfo.addNativeTypeMapping(Types.ARRAY, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DISTINCT, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.NULL, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.REF, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.STRUCT, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DATALINK, "NONE", Types.BINARY);

        dialectInfo.addNativeTypeMapping(Types.BIT, "INTEGER", Types.INTEGER);
        dialectInfo.addNativeTypeMapping(Types.BOOLEAN, "INTEGER", Types.INTEGER);
        dialectInfo.addNativeTypeMapping(Types.TINYINT, "INTEGER", Types.INTEGER);
        dialectInfo.addNativeTypeMapping(Types.SMALLINT, "INTEGER", Types.INTEGER);
        dialectInfo.addNativeTypeMapping(Types.BINARY, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.BLOB, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.CLOB, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.VARCHAR, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.LONGNVARCHAR, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.LONGVARCHAR, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.NVARCHAR, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.NCHAR, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.CHAR, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.FLOAT, "FLOAT", Types.FLOAT);
        dialectInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "NONE", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DATE, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.TIME, "TEXT", Types.VARCHAR);
        dialectInfo.addNativeTypeMapping(Types.TIMESTAMP, "TEXT", Types.VARCHAR);

        dialectInfo.setHasSize(Types.CHAR, false);
        dialectInfo.setHasSize(Types.VARCHAR, false);
        dialectInfo.setHasSize(Types.BINARY, false);
        dialectInfo.setHasSize(Types.VARBINARY, false);

        dialectInfo.setHasPrecisionAndScale(Types.DECIMAL, false);
        dialectInfo.setHasPrecisionAndScale(Types.NUMERIC, false);

        this.openHelper = openHelper;

        dialectInfo.setDateOverridesToTimestamp(false);
        dialectInfo.setEmptyStringNulled(false);
        dialectInfo.setBlankCharColumnSpacePadded(true);
        dialectInfo.setNonBlankCharColumnSpacePadded(false);
        dialectInfo.setRequiresAutoCommitFalseToSetFetchSize(false);

        this.sqlTemplate = new SQLiteSqlTemplate(openHelper, this);
        this.tableBuilder = new SQLiteTableBuilder(this);
        this.dataCaptureBuilder = new SQLiteDataCaptureBuilder(this);
    }

    public Database findDatabase(String catalogName, String schemaName) {
        Database database = new Database();
        List<Table> tables = findTables(catalogName, schemaName, false);
        for (Table table : tables) {
            database.addTable(table);
        }
        return database;
    }

    public List<Table> findTables(String catalogName, String schemaName, boolean useCached) {
        List<String> tableNames = sqlTemplate
                .query("select tbl_name from sqlite_master where type='table'",
                        SqlConstants.STRING_MAPPER);
        List<Table> tables = new ArrayList<Table>(tableNames.size());
        for (String tableName : tableNames) {
            Table table = findTable(catalogName, schemaName, tableName, useCached);
            if (table != null) {
                tables.add(table);
            }
        }
        return tables;
    }

    @Override
    protected Table readTable(String catalogName, String schemaName, String tableName,
            boolean caseSensitive, boolean makeAllColumnsPKsIfNoneFound) {
        Table table = null;
        List<Column> columns = sqlTemplate.query("pragma table_info(" + tableName + ")",
                COLUMN_MAPPER);

        if (columns != null && columns.size() > 0) {
            table = new Table(tableName);
            for (Column column : columns) {
                table.addColumn(column);
            }

            List<Index> indexes = sqlTemplate.query("pragma index_list(" + tableName + ")",
                    INDEX_MAPPER);
            for (Index index : indexes) {
                List<IndexColumn> indexColumns = sqlTemplate.query(
                        "pragma index_info(" + index.getName() + ")", INDEX_COLUMN_MAPPER);
                for (IndexColumn indexColumn : indexColumns) {
                    index.addColumn(indexColumn);
                    indexColumn.setColumn(table.getColumn(indexColumn.getName()));
                }
                if (!(index.hasAllPrimaryKeys() && index.getName().toLowerCase()
                        .contains("autoindex"))) {
                    table.addIndex(index);
                }
            }
        }

        return table;
    }

    public ISqlTemplate getSqlTemplate() {
        return this.sqlTemplate;
    }

    public boolean isDataIntegrityException(Exception ex) {
        return ex instanceof SQLiteConstraintException;
    }

    public boolean supportsBatchUpdates() {
        return true;
    }

    static class ColumnMapper extends AbstractSqlRowMapper<Column> {
        public Column mapRow(Row row) {
            Column col = new Column((String) row.get("name"), toJdbcType((String) row.get("type")),
                    null, false, booleanValue(row.get("notnull")), booleanValue(row.get("pk")));
            col.setDefaultValue((String) row.get("dflt_value"));
            return col;
        }

        public String toJdbcType(String colType) {
            colType = colType == null ? "TEXT" : colType.toUpperCase();
            if (colType.startsWith("INT")) {
                colType = TypeMap.INTEGER;
            } else if (colType.startsWith("NUM")) {
                colType = TypeMap.NUMERIC;
            } else if (colType.startsWith("TEXT") || colType.contains("CHAR")) {
                colType = TypeMap.VARCHAR;
            } else if (colType.startsWith("FLOAT")) {
                colType = TypeMap.FLOAT;
            } else {
                colType = TypeMap.VARCHAR;
            }
            return colType;
        }

    }

    static class IndexMapper extends AbstractSqlRowMapper<Index> {
        public Index mapRow(Row row) {
            boolean unique = booleanValue(row.get("unique"));
            String name = (String) row.get("name");
            if (unique) {
                return new UniqueIndex(name);
            } else {
                return new NonUniqueIndex(name);
            }
        }
    }

    static class IndexColumnMapper extends AbstractSqlRowMapper<IndexColumn> {
        public IndexColumn mapRow(Row row) {
            IndexColumn column = new IndexColumn();
            column.setName((String) row.get("name"));
            column.setOrdinalPosition(intValue(row.get("cid")));
            return column;
        }
    }

}
