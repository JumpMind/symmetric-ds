package org.jumpmind.db.platform.ingres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTransaction;
import org.jumpmind.db.sql.SqlException;

public class IngresDdlReader extends AbstractJdbcDdlReader {

    public IngresDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern(null);
        setDefaultColumnPattern(null);
    }
    
    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.equalsIgnoreCase("INTEGER1")) {
            return Types.TINYINT;
        } else if (typeName != null && typeName.equalsIgnoreCase("INTEGER2")) {
            return Types.SMALLINT;
        } else if (typeName != null && typeName.equalsIgnoreCase("INTEGER4")) {
            return Types.INTEGER;
        } else if (typeName != null && typeName.equalsIgnoreCase("INTEGER8")) {
            return Types.BIGINT;
        } else if (typeName != null && typeName.equalsIgnoreCase("LONG VARCHAR")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.equalsIgnoreCase("LONG NVARCHAR")) {
            return Types.LONGNVARCHAR;
        } else if (typeName != null && typeName.equalsIgnoreCase("TEXT")) {
            return Types.VARCHAR;
        } else if (typeName != null && typeName.equalsIgnoreCase("LONG BYTE")) {
            return Types.LONGVARBINARY;
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }
    
    @Override
    public Database readTables(final String catalog, final String schema, final String[] tableTypes) {
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplateDirty();
        ISqlTransaction transaction = sqlTemplate.startSqlTransaction();
        Database database = null;
        try {
            database = postprocessModelFromDatabase(((JdbcSqlTransaction)transaction)
                    .executeCallback(new IConnectionCallback<Database>() {
                        public Database execute(Connection connection) throws SQLException {
                            Database db = new Database();
                            db.setName(Table.getFullyQualifiedTablePrefix(catalog, schema));
                            db.setCatalog(catalog);
                            db.setSchema(schema);
                            db.addTables(readTables(connection, catalog, schema, tableTypes));
                            db.initialize();
                            return db;
                        }
                    }));
            transaction.commit();
        } catch(Throwable e) {
            if(transaction != null) {
                transaction.rollback();
            }
            if(e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            if(transaction != null) {
                transaction.close();
            }
        }
        
        return database;
    }

    @Override
    public Table readTable(final String catalog, final String schema, final String table) {
        Table tableObject = null;
        try {
            log.debug("reading table: " + table);
            JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplateDirty();
            ISqlTransaction transaction = sqlTemplate.startSqlTransaction();
            try {
                tableObject = postprocessTableFromDatabase(((JdbcSqlTransaction)transaction).executeCallback(new IConnectionCallback<Table>() {
                public Table execute(Connection connection) throws SQLException {
                    DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();
                    metaData.setMetaData(connection.getMetaData());
                    metaData.setCatalog(catalog);
                    metaData.setSchemaPattern(schema);
                    metaData.setTableTypes(null);
    
                    ResultSet tableData = null;
                    try {
                        log.debug("getting table metadata for {}", table);
                        tableData = metaData.getTables(getTableNamePattern(StringUtils.lowerCase(table)));
                        log.debug("done getting table metadata for {}", table);
                        if (tableData != null && tableData.next()) {
                            Map<String, Object> values = readMetaData(tableData, initColumnsForTable());
                            return readTable(connection, metaData, values);
                        } else {
                            close(tableData);
                            tableData = metaData.getTables(getTableNamePattern(StringUtils.upperCase(table)));
                            if (tableData != null && tableData.next()) {
                                Map<String, Object> values = readMetaData(tableData, initColumnsForTable());
                                return readTable(connection, metaData, values);
                            }
                            log.debug("table {} not found", table);
                            return null;
                        }
                    } finally {
                        close(tableData);
                    }
                }
            }));
            } catch(Throwable e) {
                if(transaction != null) {
                    transaction.rollback();
                }
                if(e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            } finally {
                if(transaction != null) {
                    transaction.close();
                }
            }
        } catch (SqlException e) {
            if (e.getMessage()!=null && StringUtils.containsIgnoreCase(e.getMessage(), "does not exist")) {
                return null;
            } else {
                log.error("Failed to get metadata for {}", Table.getFullyQualifiedTableName(catalog, schema, table));
                throw e;
            }
        }
        return tableObject;
    }
    
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException
    {
        Table table = super.readTable(connection, metaData, values);
        String schema = table.getSchema();
        // select column_name, column_always_ident, column_bydefault_ident
        // from iicolumns
        // where table_name='test_uppercase_table' and (column_always_ident='Y' OR column_bydefault_ident='Y')
        // column types that can have identity generators are int and bigint
        // only one column per table can have identity generator
        
        boolean setTableOwner = schema != null && schema.length() > 0 ? true : false;
        StringBuilder sql = new StringBuilder("select column_name,  column_always_ident, column_bydefault_ident, column_default_val ")
                .append("from iicolumns ")
                .append("where table_name=? ")
                .append(schema != null && schema.length() > 0 ? " and table_owner=? " : "");
        Object[] args = new Object[] {table.getName()};
        String columnName = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(sql.toString());
            ps.setString(1, table.getName());
            if(setTableOwner) {
                ps.setString(2, schema);
            }
            ps.setQueryTimeout(((IngresDatabasePlatform) platform).getSettings().getQueryTimeout());

            rs = ps.executeQuery();

            processAdditionalColumnInformation(table, rs);
        } catch (SQLException e) {
            log.error(sql.toString(), args, e);
            throw e;
        } finally {
            close(rs);
            close(ps);
        }
        return table;
    }
    
    private void processAdditionalColumnInformation(Table table, ResultSet rs) throws SQLException {
        while(rs.next()) {
            String columnName = StringUtils.trim(rs.getString("COLUMN_NAME"));
            String columnAlwaysIdent = StringUtils.trim(rs.getString("COLUMN_ALWAYS_IDENT"));
            String columnBydefaultIdent = StringUtils.trim(rs.getString("COLUMN_BYDEFAULT_IDENT"));
            String columnDefaultVal = StringUtils.trim(rs.getString("COLUMN_DEFAULT_VAL"));
            for(Column column : table.getColumnsAsList()) {
                if(column.getName().equalsIgnoreCase(columnName)) {
                    if((columnAlwaysIdent != null && columnAlwaysIdent.equalsIgnoreCase("Y")) ||
                            columnBydefaultIdent  != null && columnBydefaultIdent.equalsIgnoreCase("Y"))
                    {
                        column.setAutoIncrement(true);
                    } else if(columnDefaultVal != null) {
                        column.setDefaultValue(columnDefaultVal);
                    }
                    break;
                }
            }
        }
    }
}
