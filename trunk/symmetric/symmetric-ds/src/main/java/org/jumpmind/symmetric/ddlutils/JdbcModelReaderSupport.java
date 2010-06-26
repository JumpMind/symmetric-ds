package org.jumpmind.symmetric.ddlutils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ddl.platform.DatabaseMetaDataWrapper;
import org.jumpmind.symmetric.ddl.platform.JdbcModelReader;
import org.springframework.jdbc.support.JdbcUtils;

/**
 * Common methods needed to fix bugs and enhance the DdlUtils JdbcModelReader
 * class.
 */
public class JdbcModelReaderSupport extends JdbcModelReader {

    public JdbcModelReaderSupport(Platform platform) {
        super(platform);
    }

    /**
     * Read a single table from database
     */
    @SuppressWarnings("unchecked")
    public Table readTable(Connection conn, String catalogName, String schemaName,
            String tableName, boolean caseSensitive) throws SQLException {
        Table retTable = null;
        DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();
        metaData.setMetaData(conn.getMetaData());
        if (caseSensitive) {
            metaData.setCatalog(catalogName);
            metaData.setSchemaPattern(schemaName);
        }

        ResultSet tableData = null;
        try {
            tableData = metaData.getTables(tableName);
            while (tableData != null && tableData.next()) {
                Map values = readColumns(tableData, getColumnsForTable());
                Table table = readTable(metaData, values);
                if (doesMatch(table, catalogName, schemaName, tableName, caseSensitive)) {
                    retTable = table;
                }
            }
        } finally {
            JdbcUtils.closeResultSet(tableData);
        }

        return retTable;
    }

    public Table readTable(Connection conn, String catalogName, String schemaName, String tableName)
            throws SQLException {
        return readTable(conn, catalogName, schemaName, tableName, true);
    }

    protected boolean doesMatch(Table table, String catalogName, String schemaName,
            String tableName, boolean caseSensitive) {
        if (caseSensitive) {
            return ((catalogName == null || (catalogName != null && catalogName.equals(table
                    .getCatalog())))
                    && (schemaName == null || (schemaName != null && schemaName.equals(table
                            .getSchema()))) && table.getName().equals(tableName));
        } else {
            return ((catalogName == null || (catalogName != null && catalogName
                    .equalsIgnoreCase(table.getCatalog())))
                    && (schemaName == null || (schemaName != null && schemaName
                            .equalsIgnoreCase(table.getSchema()))) && table.getName()
                    .equalsIgnoreCase(tableName));
        }
    }

}
