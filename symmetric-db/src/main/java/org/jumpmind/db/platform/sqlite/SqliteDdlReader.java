package org.jumpmind.db.platform.sqlite;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.NonUniqueIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.model.UniqueIndex;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlReader;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlConstants;

public class SqliteDdlReader implements IDdlReader {

    final static ColumnMapper COLUMN_MAPPER = new ColumnMapper();
    final static IndexMapper INDEX_MAPPER = new IndexMapper();
    final static IndexColumnMapper INDEX_COLUMN_MAPPER = new IndexColumnMapper();

    protected IDatabasePlatform platform;

    public SqliteDdlReader(IDatabasePlatform platform) {
        this.platform = platform;
    }

    public Database readTables(String catalog, String schema, String[] tableTypes) {
        List<String> tableNames = platform.getSqlTemplate()
                .query("select tbl_name from sqlite_master where type='table'",
                        SqlConstants.STRING_MAPPER);
        Database database = new Database();
        for (String tableName : tableNames) {
            Table table = readTable(catalog, schema, tableName);
            if (table != null) {
                database.addTable(table);
            }
        }
        return database;
    }
    
    protected void checkForAutoIncrementColumn(List<Column> columns, String tableName) {
        String ddl = platform.getSqlTemplate().queryForObject("select sql from sqlite_master where tbl_name=?", String.class, tableName);
        if (StringUtils.isNotBlank(ddl)) {
            String[] split = ddl.split(",");
            for (String string : split) {
                for (Column col : columns) {
                    if (string.contains(col.getName()) && string.toUpperCase().contains("AUTOINCREMENT")) {
                        col.setAutoIncrement(true);
                        return;
                    }
                }
            }
        }
        
    }
    
    public Table readTable(String catalog, String schema, String tableName, String sql) {
        throw new NotImplementedException();
    }

    public Table readTable(String catalog, String schema, String tableName) {
        Table table = null;
        
        List<Column> columns = platform.getSqlTemplate().query(
                "pragma table_info(" + tableName + ")", COLUMN_MAPPER);
        
        checkForAutoIncrementColumn(columns, tableName);
        
        if (columns != null && columns.size() > 0) {
            table = new Table(tableName);
            for (Column column : columns) {
                table.addColumn(column);
            }

            List<IIndex> indexes = platform.getSqlTemplate().query(
                    "pragma index_list(" + tableName + ")", INDEX_MAPPER);
            for (IIndex index : indexes) {
                List<IndexColumn> indexColumns = platform.getSqlTemplate().query(
                        "pragma index_info(" + index.getName() + ")", INDEX_COLUMN_MAPPER);
                for (IndexColumn indexColumn : indexColumns) {
                    index.addColumn(indexColumn);
                    indexColumn.setColumn(table.getColumnWithName(indexColumn.getName()));
                }

                if (!(index.hasAllPrimaryKeys() && index.getName().toLowerCase()
                        .contains("autoindex"))) {
                    table.addIndex(index);
                }
            }
        }

        return table;
    }
    
    public List<String> getCatalogs() {
        return new ArrayList<String>(0);
    }
    
    
    public List<String> getSchemas(String catalog) {
        return new ArrayList<String>(0);
    }

    static class ColumnMapper extends AbstractSqlRowMapper<Column> {
        public Column mapRow(Row row) {
            Column col = new Column((String) row.get("name"), booleanValue(row.get("pk")));
            col.setMappedType(toJdbcType((String) row.get("type")));
            col.setRequired(booleanValue(row.get("notnull")));
            col.setDefaultValue(scrubDefaultValue((String) row.get("dflt_value")));
            return col;
        }

        protected String scrubDefaultValue(String defaultValue) {
            if (defaultValue != null && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
            }
            return defaultValue;
        }

        public String toJdbcType(String colType) {
            colType = colType == null ? "TEXT" : colType.toUpperCase();
            if (colType.startsWith("INT")) {
                colType = TypeMap.INTEGER;
            } else if (colType.startsWith("NUM")) {
                colType = TypeMap.NUMERIC;
            } else if (colType.startsWith("BLOB")) {
                colType = TypeMap.BLOB;   
            } else if (colType.startsWith("CLOB")) {
                colType = TypeMap.CLOB;   
            } else if (colType.startsWith("TEXT") || colType.contains("CHAR")) {
                colType = TypeMap.VARCHAR;
            } else if (colType.startsWith("FLOAT")) {
                colType = TypeMap.FLOAT;
            } else if (colType.startsWith("DOUBLE")) {
                colType = TypeMap.DOUBLE;
           } else if (colType.startsWith("DECIMAL")) {
                colType = TypeMap.DECIMAL; 
            } else if (colType.startsWith("DATE")) {
                colType = TypeMap.DATE; 
            } else if (colType.startsWith("TIMESTAMP")) {
                colType = TypeMap.TIMESTAMP; 
            } else if (colType.startsWith("TIME")) {
                colType = TypeMap.TIME; 
            } else {
                colType = TypeMap.VARCHAR;
            }
            return colType;
        }

    }

    static class IndexMapper extends AbstractSqlRowMapper<IIndex> {
        public IIndex mapRow(Row row) {
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
            column.setOrdinalPosition(intValue(row.get("seqno")));
            return column;
        }
    }
    
    

}
