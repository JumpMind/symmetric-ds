package org.jumpmind.symmetric.jdbc.db;

import java.io.StringWriter;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.db.AbstractDbPlatform;
import org.jumpmind.symmetric.core.model.Database;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.jdbc.sql.ILobHandler;
import org.jumpmind.symmetric.jdbc.sql.JdbcSqlConnection;

abstract public class AbstractJdbcDbPlatform extends AbstractDbPlatform implements IJdbcDbPlatform {

    protected DataSource dataSource;

    protected JdbcModelReader jdbcModelReader;   

    public AbstractJdbcDbPlatform(DataSource dataSource, Parameters parameters) {
        super(parameters);
        this.dataSource = dataSource;
    }
    
    public ISqlConnection getSqlConnection() {
        return getJdbcSqlConnection();
    }
    
    public JdbcSqlConnection getJdbcSqlConnection() {
        return new JdbcSqlConnection(this, parameters);
    }
    
    public Database findDatabase(String catalogName, String schemaName) {
        return jdbcModelReader.getDatabase(catalogName, schemaName, null);
    }    
    
    public String getAlterScriptFor(Table... tables) {
        StringWriter writer = new StringWriter();
        sqlBuilder.setWriter(writer);
        Database desiredModel = new Database();
        desiredModel.addTables(tables);
        
        Database currentModel = new Database();
        for (Table table : tables) {
            currentModel.addTable(jdbcModelReader.readTable(table.getCatalogName(), table.getSchemaName(), table.getTableName(), false, false));
        }
                
        sqlBuilder.alterDatabase(currentModel, desiredModel);

        return writer.toString();
    }

    public Table findTable(String tableName, Parameters parameters) {
        return findTable(null, null, tableName, false, parameters);
    }

    public Table findTable(String catalogName, String schemaName, String tableName,
            boolean useCache, Parameters parameters) {
        Table cachedTable = cachedModel.findTable(catalogName, schemaName, tableName);
        if (cachedTable == null || !useCache) {
            Table justReadTable = jdbcModelReader.readTable(catalogName, schemaName, tableName,
                    parameters.is(Parameters.DB_METADATA_IGNORE_CASE, true),
                    parameters.is(Parameters.DB_USE_ALL_COLUMNS_AS_PK_IF_NONE_FOUND, false));
            
            if (cachedTable != null) {
                cachedModel.removeTable(cachedTable);
            }

            if (justReadTable != null) {
                cachedModel.addTable(justReadTable);
            }

            cachedTable = justReadTable;
        }
        return cachedTable;
    }

    public java.util.List<Table> findTables(String catalog, String schema, Parameters parameters) {
        return null;
    };

    protected void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public boolean isDataIntegrityException(Exception ex) {
        boolean integrityError = false;
        if (ex instanceof SQLException) {
            int sqlErrorCode = ((SQLException) ex).getErrorCode();
            int[] codes = getDataIntegritySqlErrorCodes();
            for (int i : codes) {
                if (sqlErrorCode == i) {
                    integrityError = true;
                    break;
                }
            }
        }
        return integrityError;
    }

    public ILobHandler getLobHandler() {
        return null;
    }

    abstract protected int[] getDataIntegritySqlErrorCodes();

}
