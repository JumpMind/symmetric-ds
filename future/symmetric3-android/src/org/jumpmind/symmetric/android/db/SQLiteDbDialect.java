package org.jumpmind.symmetric.android.db;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.db.AbstractDbDialect;
import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.SqlConstants;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;

import android.database.sqlite.SQLiteConstraintException;
import android.database.sqlite.SQLiteOpenHelper;

public class SQLiteDbDialect extends AbstractDbDialect {

    protected SQLiteOpenHelper openHelper;

    protected SQLiteSqlTemplate sqlTemplate;

    public SQLiteDbDialect(SQLiteOpenHelper openHelper, Parameters parameters) {
        super(parameters);

        dialectInfo.setNonPKIdentityColumnsSupported(false);
        dialectInfo.setIdentityOverrideAllowed(false);
        dialectInfo.setSystemForeignKeyIndicesAlwaysNonUnique(true);
        dialectInfo.setNullAsDefaultValueRequired(false);
        dialectInfo.addNativeTypeMapping(Types.ARRAY, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DISTINCT, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.NULL, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.REF, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.STRUCT, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.DATALINK, "BINARY", Types.BINARY);

        dialectInfo.addNativeTypeMapping(Types.BIT, "BOOLEAN", Types.BIT);
        dialectInfo.addNativeTypeMapping(Types.TINYINT, "SMALLINT", Types.TINYINT);
        dialectInfo.addNativeTypeMapping(Types.SMALLINT, "SMALLINT", Types.SMALLINT);
        dialectInfo.addNativeTypeMapping(Types.BINARY, "BINARY", Types.BINARY);
        dialectInfo.addNativeTypeMapping(Types.BLOB, "BLOB", Types.BLOB);
        dialectInfo.addNativeTypeMapping(Types.CLOB, "CLOB", Types.CLOB);
        dialectInfo.addNativeTypeMapping(Types.FLOAT, "DOUBLE", Types.DOUBLE);
        dialectInfo.addNativeTypeMapping(Types.JAVA_OBJECT, "OTHER");

        dialectInfo.setDefaultSize(Types.CHAR, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.VARCHAR, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.BINARY, Integer.MAX_VALUE);
        dialectInfo.setDefaultSize(Types.VARBINARY, Integer.MAX_VALUE);

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

    // http://www.xerial.org/trac/Xerial/browser/XerialJ/trunk/sqlite-jdbc/src/main/java/org/sqlite/MetaData.java
    @Override
    protected Table readTable(String catalogName, String schemaName, String tableName,
            boolean caseSensitive, boolean makeAllColumnsPKsIfNoneFound) {

        // PRAGMA index_info(index-name);
        //
        // This pragma returns one row each column in the named index. The first
        // column of the result is the rank of the column within the index. The
        // second column of the result is the rank of the column within the
        // table. The third column of output is the name of the column being
        // indexed.
        //
        // PRAGMA index_list(table-name);
        //
        // This pragma returns one row for each index associated with the given
        // table. Columns of the result set include the index name and a flag to
        // indicate whether or not the index is UNIQUE.
        List<Column> columns = sqlTemplate.query("pragma table_info(" + tableName + ")",
                new ColumnMapper());
        Table table = new Table(tableName);
        for (Column column : columns) {
            table.addColumn(column);
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

    class ColumnMapper implements ISqlRowMapper<Column> {
        public Column mapRow(Map<String, Object> row) {
            Column col = new Column((String) row.get("name"), toJdbcType((String) row.get("type")),
                    null, false, booleanValue("notnull"), booleanValue("pk"));
            col.setDefaultValue((String) row.get("cid"));
            return col;
        }

        public String toJdbcType(String colType) {
            colType = colType == null ? "TEXT" : colType.toUpperCase();
            if (colType.equals("INT") || colType.equals("INTEGER")) {
                colType = TypeMap.INTEGER;
            } else if (colType.equals("TEXT")) {
                colType = TypeMap.VARCHAR;
            } else if (colType.equals("FLOAT")) {
                colType = TypeMap.FLOAT;
            } else {
                colType = TypeMap.VARCHAR;
            }
            return colType;
        }

        protected boolean booleanValue(Object v) {
            return v.equals("1");
        }
    }

}
